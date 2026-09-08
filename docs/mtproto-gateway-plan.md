# MTProto Session Gateway — תכנית הטמעה

ממופה למפרט המלא (F1-F31, N1-N6, E1-E7) ול-FAQ הרשמי של טלגרם.
מבוסס על ניתוח שלוש השכבות: Auth, Rolling, Rate.

## מודל הבשלות

```
Phase 0 — מה שיש היום (מכסה חלקית)
  ├── Bot API gateway (מדבר HTTP, לא MTProto)
  └── mtproto/patch.py (פותר auth, לא פותר rolling/rate)

Phase 1 — MTProto Session Core     ← ה-MVP האמיתי
Phase 2 — Rate Guard for MTProto
Phase 3 — Production Hardening
Phase 4 — Multi-Consumer (V2)

כל Phase עומדת בפני עצמה ומחזירה ערך מידי.
Phase 1 לבדה פותרת את קריטריוני הקבלה 1, 2, 5, 6.
Phase 2 מוסיפה את 3, 4.
Phase 3 סוגרת את N5, N6 ואת E1-E7.
```

## Phase 1: MTProto Session Core

**מטרה:** תהליך אחד שהוא הלקוח היחיד של MTProto. האפליקציה מדברת אליו ב-HTTP/WS. מחזור חיים נפרד לחלוטין.

**מה נבנה:**

| רכיב | מימוש | ממפה ל |
|---|---|---|
| Session Holder | Pyrogram Client ארוך-חיים לכל טוקן, session file על volume | F1, F2, F4, F5 |
| Update State | ‏pts/qts/date/seq נשמרים עם הסשן לדיסק | F3 |
| HTTP API | ‏POST /session/{alias}/{method} — פרוקסי שקוף ל-Pyrogram methods | F10, F12 |
| WS Updates | ‏WebSocket push של updates לאפליקציה | F11 |
| Pull Updates | ‏HTTP long-poll (לתאימות עם ספריות polling) | F11 |
| Lifecycle | ‏compose משלו, רשת חיצונית, ווליום ייעודי | F6 |
| App Attach | ‏האפליקציה מתחברת לשער ≤ 1 שנייה, טלגרם לא מרגיש | F7, F14 |
| Rolling | ‏רפליקה חדשה מתחברת לשער; הישנה מתנתקת; טלגרם לא רואה | F8 |
| Restart | ‏safe-restart: drain ← save session ← up ← resume מאותו auth_key | F9 |
| Readiness | ‏/healthz (תהליך) + ‏/readyz (סשן מחובר לטלגרם) | F27 |

**חוזה צרכן (HTTP):**
```
POST /session/{alias}/send_message
  {"chat_id": 123, "text": "hello"}
  → 200 {"ok": true, "result": {...}}     — נשלח
  → 429 {"ok": false, "parameters": {"retry_after": 3}}  — Rate Guard
  → 503 {"ok": false, "description": "session reconnecting"}  — טלגרם

WebSocket /session/{alias}/updates
  ← {"update_id": ..., "message": {...}}   — push

GET /session/{alias}/health
  → {"state": "live", "connection_age_s": 3600, ...}
```

**מה לא נבנה ב-Phase 1:**
- Rate Guard מלא (Phase 2)
- תור עדכונים על דיסק (Phase 3)
- כמה צרכנים לאותו סשן (Phase 4)

**קריטריוני קבלה של Phase 1:**
- [ ] 15 deploys ב-20 דק': אפס ImportBotAuthorization (§8.1)
- [ ] רולינג עם overlap ‏5 שנ': אפס AUTH_KEY_DUPLICATED (§8.2)
- [ ] אפליקציה כבוית 90 שנ': ממשיך מ-offset בלי פסק זמן (§8.5)
- [ ] compose down על האפליקציה לא עוצר את השער (§8.6)

---

## Phase 2: Rate Guard for MTProto

**מטרה:** השער אוכף FAQ + FLOOD_WAIT לפי peer/method. האפליקציה לא יכולה להפיל את הבוט לכולם.

| רכיב | מימוש | ממפה ל |
|---|---|---|
| FAQ Buckets | ‏1/s פרטי (burst ‏3), ‏20/דק' קבוצה, ‏30/שנ' גלובלי | F15, F16 |
| Fast Path | ‏answer_callback / answer_inline — מחוץ לדלי שליחה | F17 |
| Paid Broadcast | ‏opt-in כפול, דלי ‏1000/שנ' | F18 |
| FLOOD_WAIT Real | ‏עוצר רק את הדלי של peer+method ל-retry_after | F19 |
| Policies | ‏queue (ברירת מחדל) / reject (429 סינתטי) | F20 |
| Silent Methods | ‏resolve_username, getParticipants — דלי נפרד | F21 |
| Adaptive Backoff | ‏אחרי FLOOD_WAIT אמיתי — המתנה גדלה, לא ניסיון מיידי | F22 |

**הבדל מ-Bot API Rate Guard:**
- MTProto מחזיר FLOOD_WAIT_X (לא 429 עם JSON) — צריך לפרסר את ה-error string
- FLOOD_PEER_WAIT_X — פר-פר מדויק, לא רק פר-צ'אט
- SLOWMODE_WAIT_X — מגבלת של הקבוצה עצמה, לא של הבוט
- צריך לכבד את כולם ולהחזיר retry_after מדויק לאפליקציה

---

## Phase 3: Production Hardening

| רכיב | מימוש | ממפה ל |
|---|---|---|
| Queue on Disk | ‏SQLite WAL לתור עדכונים — שורד ריסטארט שער | N6 |
| Per-Peer Egress Queue | ‏צ'אט אחד לא חונק את כולם | E1 |
| Idempotency | ‏מפתח מהאפליקציה — רפליקה חדשה לא שולחת כפיל | E2 |
| Drain | ‏ניקוז מפורש לפני כיבוי | E3 |
| Pause Sending | ‏מצב "קבל בלי לשלוח" בלי לנתק סשן | E4 |
| Prometheus | ‏/metrics עם per-session gauges/counters | F26 |
| Deploy-to-First-Message | ‏מטריקת SLA של הרולינג | E7 |
| Queue Overflow | ‏drop_oldest / reject / backpressure מפורש | E6 |

---

## Phase 4: Multi-Consumer (V2) — מותר לדחות

| רכיב | מימוש |
|---|---|
| Lease | ‏צרכן פעיל אחד + failover אוטומטי |
| Replicas | ‏אפליקציות מרובות מתחלפות על אותו סשן דרך השער |
| Sharding | ‏שערים מרובים, טוקנים זרים |

---

## אילוצי טלגרם — חוקי ברזל (מהמפרט §3)

1. **auth_key אחד = חיבור ראשי אחד.** חריגה ← AUTH_KEY_DUPLICATED. השער הוא החיבור היחיד.
2. **ImportBotAuthorization תכוף ← FLOOD_WAIT ארוך.** לעולם לא בזמן deploy.
3. **FAQ:** ‏≤1/שנ' פרטי, ‏≤20/דק' קבוצה, ‏~30/שנ' גלובלי. השער אוכף.
4. **MTProto:** ‏FLOOD_WAIT_X, FLOOD_PEER_WAIT_X, SLOWMODE_WAIT_X. מכבדים retry_after אמיתי.
5. **אותו .session לא בשני תהליכים.** השער הוא התהליך היחיד.
6. **Webhook/polling XOR** — אם נוגעים ב-Bot API בכלל.

---

## מה דורש שינוי ב-kot (כן/לא)

| שלב | שינוי בקוד kot | מה נדרש |
|---|---|---|
| Phase 1 (MVP) | **כן — שכבת transport** | החלפת קריאות Pyrogram ישירות ב-HTTP לשער. לא לוגיקה, רק transport. |
| Phase 2 | לא | ‏Rate Guard שקוף לאפליקציה |
| Phase 3 | לא | ‏הכל בשער |
| Phase 4 | אופציונלי | ‏lease client אם רוצים כמה רפליקות |

**ההנחה היא ש-Phase 1 כן דורש שינוי transport ב-kot** — כי אין דרך ליירט MTProto בשקיפות מלאה בלי שהאפליקציה תדבר HTTP במקום Pyrogram ישיר. זה שונה מה-patch שבנינו (שהיה zero-change) — אבל ה-patch פותר רק auth, לא rolling ולא rate. המפרט שלך צודק: זה לא מספיק.

---

## סדר עדיפויות מומלץ

```
עכשיו → Phase 1 (שבוע 1-2):  Session Core + HTTP API + WS
     → Phase 2 (שבוע 2-3):  Rate Guard FAQ + FLOOD_WAIT
     → Phase 3 (שבוע 3-4):  Queue disk + Prometheus + safe-restart
     → Phase 4 (מאוחר יותר): Multi-consumer
```

Phase 1 לבדה = אפס FloodWait ב-deploy + רולינג בלי AUTH_KEY_DUPLICATED.
Phase 2 מוסיפה = הבוט לא נופל זמנית לכולם בגלל צ'אט אחד עמוס.
Phase 3 מוסיפה = פרודקשן אמיתי: metrics, persistence, drain.
