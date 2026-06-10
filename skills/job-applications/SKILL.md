---
name: job-applications
description: Apply to job postings through ATS forms. Use when the user asks to apply to jobs, fill job applications, process a shortlist, handle Greenhouse/Lever/Ashby/Workday-style forms, upload resumes, answer work authorization or EEO questions, retrieve application security codes, or continue a batch of applications.
---

# Skill: Job Applications

Pragmatic workflow for applying to jobs on behalf of a user through ATS forms.

## Required Tools

- Use the `agent-browser` CLI for application pages (the launcher prepares the session).
- Use the `gws` CLI for Gmail when verification-code emails are needed, and only to retrieve application verification/security codes or user-requested application emails.

## Operating Principles

- Submit completed applications when the available user profile information is sufficient.
- Do not invent facts. If a required answer cannot be inferred from the resume, known preferences, or prior user instructions, ask one short clarifying question.
- Prefer truthful, conservative answers for authorization, sponsorship, location, relocation, start date, experience, and demographic questions.
- For voluntary EEO/demographic fields, choose decline, prefer not to answer, or equivalent unless the user explicitly provided preferences.
- If a role has a hard blocker such as required citizenship, clearance, untrue location commitment, unavailable role, expired session, captcha failure, or anti-spam rejection, mark it blocked and move on.
- Do not use generated cover letters unless the user asks for cover letters.
- Keep progress updates concise: submitted, blocked, needs code, or needs user input.

## Default User Profile Fields

Before applying, read the persisted applicant profile first:

```bash
/Users/edwarddgao/openapply/config/applicant-profile.json
```

Treat this profile as the source of truth for recurring applicant facts. Do not print or dump the full profile into logs; read only the fields needed for the application. If the profile lacks a required answer, derive it from the resume or prior user instruction, then ask before submitting if still unknown.

Required profile fields:

- Legal name
- Email
- Phone
- Location
- LinkedIn
- GitHub or portfolio
- Resume PDF path
- Degree, school, graduation date
- Work authorization by country
- Sponsorship requirement by country
- Earliest start date
- Relocation and onsite willingness
- Years of full-time professional experience
- Transcript availability
- Desired salary / compensation preference

If any field is unknown and required, ask before submitting.

## Browser Workflow

1. Prefer the launcher-prepared session (scripts/apply.py opens the application URL before handing off). When opening a URL yourself, use:
   `agent-browser --headed --session-name openapply --args "--disable-blink-features=AutomationControlled" open <url>`.
2. If reusing an existing daemon, remember launch flags are ignored after the daemon starts. Close/restart that session before changing `--profile`, `--headed`, or `--args`.
3. Open the application URL with `agent-browser` in the active application session.
4. Inspect the page with the text accessibility tree first: `agent-browser snapshot -i --compact` (and add `--depth N` or `-s <selector>` to scope long forms). Use this to identify ATS type and required fields. Do **not** take a full-page screenshot — long ATS forms (Greenhouse, Workday) routinely exceed 2000px and Claude/Codex will reject the image, blocking the whole role. Only use `agent-browser screenshot` (without `--full`) on a small selector when the text snapshot is genuinely insufficient (e.g., visual captcha or canvas-based widget). Re-snapshot after meaningful DOM changes; do not periodically poll with screenshots.
5. Fill basic fields first: name, email, phone, location/country.
6. Upload the resume from the known resume PDF path.
7. Fill profile links and education fields.
8. Answer work authorization and sponsorship fields from user facts.
9. Answer voluntary EEO/demographic fields with decline/prefer-not-answer unless instructed otherwise.
10. Submit the form.
11. Confirm success by checking for a confirmation URL or text such as `received your application`, `thanks for applying`, `already received`, or `confirmation`.
12. If blocked, capture the exact visible error and proceed to the next role if the user asked for batch progress.
13. Always close every `agent-browser` session created for the role before finishing, whether submitted, blocked, or needs input.

## External Codex Subagents

- `scripts/apply.py` is the launcher for batch runs: it selects roles from the shortlist, prepares one agent-browser session per role (close stale session, open the application URL, wait for settle), starts one `codex exec` subagent per role, records outcomes in `applications/status.jsonl`, and writes per-role logs to `applications/codex-subagent-logs/`.
- Assign one role per Codex subagent unless the user explicitly requests batching inside a subagent.
- Give every subagent a unique `agent-browser --session` name such as `codex-company-role-subagent`.
- For a manual one-off outside the launcher:

```bash
codex exec \
  -C "/Users/edwarddgao/openapply" \
  -s danger-full-access \
  -c 'model_reasoning_effort="low"' \
  --output-last-message "applications/codex-subagent-logs/<company-role>.txt" \
  "Apply to exactly one role: <company> - <role> at <url>. Use the already-prepared agent-browser session <unique-session>; do not relaunch the browser. Do not dump large page data. Close the browser session before finishing. Return one concise status line only: Submitted / Blocked / Needs input." \
  > "applications/codex-subagent-logs/<company-role>-run.log" 2>&1
```

- `danger-full-access` is used because subagents need the agent-browser daemon and the resume/config paths outside the sandboxed workspace. Only use it for narrow, single-role prompts.
- Redirect Codex's verbose run log to a file and read `--output-last-message` for the final status. This keeps coordinator output compact and avoids flooding the main thread with snapshots.
- Prompts should include concrete known answers and explicitly say not to read long docs or dump large page data unless needed.
- The main coordinator should close any remaining browser sessions after Codex returns, even if the subagent says it cleaned up.

## Simplify Copilot Autofill

Simplify Copilot autofill is only usable from the user's live signed-in Chrome (where the extension is installed and authenticated), driven interactively via the Chrome MCP tools. Do not attempt to load the extension or its auth state into agent-browser sessions: the extension path and saved auth-state JSON this skill previously referenced no longer exist, and injecting saved state does not produce an authenticated session. In agent-browser sessions, fill forms directly from the applicant profile.

- In the live-Chrome flow: click `Autofill this page`, wait for completion, then inspect remaining empty/invalid required fields. The agent handles any remaining required answers, file uploads, consent checkboxes, captcha/security-code flows, and final submission.
- Treat Simplify as a helper, not an authority. Verify field values before submitting, especially work authorization, sponsorship, custom questions, EEO, consent checkboxes, and stale/incorrect profile fields.

## Greenhouse Notes

- Direct job-board URLs are often more reliable than embedded iframes.
- If clicking submit does not work, try `document.querySelector('form').requestSubmit()`.
- Greenhouse security-code fallback uses 8-character codes and fields commonly named `#security-input-0` through `#security-input-7`.
- Search Gmail for security-code emails from both US and EU Greenhouse senders:

```bash
gws gmail users messages list --params '{"userId":"me","q":"newer_than:1d from:(no-reply@us.greenhouse-mail.io OR no-reply@eu.greenhouse-mail.io) subject:(Security code for your application to COMPANY)","maxResults":5}' --format json
```

- Read the message and use the snippet or HTML body to extract the code:

```bash
gws gmail users messages get --params '{"userId":"me","id":"MESSAGE_ID","format":"full"}' --format json
```

- If `gws` returns `invalid_grant: Token has been expired or revoked`, every security-code role in the batch will fail the same way. Report `blocked_on_gmail_auth` (do not ask the user for codes role-by-role); the user must re-run `gws auth login` interactively, after which these roles are retryable.

- If React select widgets show values but validation still fails, inspect `window.__remixContext.state.loaderData` for `submitPath`, `confirmationPath`, `jobPost.questions`, and `jobPost.fingerprint`. Submit via Greenhouse's JSON endpoint only when the UI is clearly broken and the payload can be built from visible user-entered values.
- Greenhouse API payload shape:

```json
{
  "job_application": {
    "first_name": "...",
    "last_name": "...",
    "email": "...",
    "phone": "...",
    "resume_url": "...",
    "resume_url_filename": "resume.pdf",
    "answers_attributes": {
      "QUESTION_ID": {
        "question_id": "QUESTION_ID",
        "priority": 0,
        "text_value": "..."
      }
    },
    "demographic_answers": [],
    "data_compliance": {},
    "attachments": {},
    "from_job_board_renderer": true,
    "employments": []
  },
  "fingerprint": "..."
}
```

- For Greenhouse yes/no select questions, use `boolean_value: 1` for Yes and `boolean_value: 0` for No when the option values are `1` and `0`.
- If Greenhouse returns `captcha-failed` with `security_code_recipient`, retrieve the email code and resubmit with `security_code` instead of a captcha token.

## Lever Notes

- Lever commonly blocks automated submissions with hCaptcha or generic verification errors; headed sessions can succeed where headless ones fail.
- Try normal UI submission first through `#btn-submit`.
- Lever forms may show a false resume-size message (`File exceeds the maximum upload size of 100MB`) even when the upload succeeds and `resumeStorageId` is populated. Do not treat that UI message as final by itself.
- Check form state before submit:

```js
({
  valid: document.querySelector('form')?.checkValidity?.(),
  hcaptchaLen: (document.querySelector('#hcaptchaResponseInput')?.value || '').length,
  resumeStorageId: document.querySelector('[name=resumeStorageId]')?.value,
  errors: Array.from(document.querySelectorAll('.error-message')).map(e => e.innerText).filter(Boolean),
})
```

- If the hCaptcha response is empty and Lever's page script exposes `captchaId`, execute that rendered widget and wait for `#hcaptchaResponseInput` to populate:

```js
hcaptcha.reset(captchaId);
hcaptcha.execute(captchaId);
```

- For Lever location fields, use `/searchLocations` with the current hCaptcha response and set both visible and hidden location fields from Lever's returned location object:

```js
const h = document.querySelector('#hcaptchaResponseInput')?.value || '';
const res = await fetch('/searchLocations?text=' + encodeURIComponent('Toronto') + '&hcaptchaResponse=' + encodeURIComponent(h), { credentials: 'include' });
const loc = (await res.json())[0];
document.querySelector('#location-input').value = loc.name;
document.querySelector('#selected-location').value = JSON.stringify(loc);
```

- If the visible submit button stays blocked by the stale resume-size UI after the form is valid and hCaptcha has a fresh token, hide the stale UI error and click Lever's hidden validated submit button:

```js
document.querySelector('.resume-upload-oversize')?.style.setProperty('display', 'none');
document.querySelector('#hcaptchaSubmitBtn')?.click();
```

- If Lever returns `Application submitted!`, `/thanks`, or `Application already received`, count the role as handled.
- If Lever still returns `There was an error verifying your application. Please try again.` after a fresh hCaptcha token and hidden validated submit, mark blocked or ask the user to solve/retry manually in the headed browser.

## Ashby Notes

Ashby embeds Google invisible reCAPTCHA v3 + SEON device fingerprinting and silently scores how human the session looks. A low score returns "Your application submission was flagged as possible spam."

### What actually drives the spam score (measured 2026-06-10 across the full ledger)

The score is dominated by **session identity**, not the browser binary or interaction style:

- Anonymous agent-browser sessions get flagged 50-86% of the time regardless of `--executable-path` (real Chrome vs bundled Chromium made no difference), cookie/state injection, or humanized interaction. Expect a high flag rate and do not burn time fighting it.
- The user's live signed-in Chrome (real profile, real Google session, driven via Chrome MCP) passed 8/8, including roles and companies that had been repeatedly flagged through agent-browser minutes earlier.
- Profile *copies* do not carry the Google login (session cookies are device-bound), so `--profile <chrome-profile>` and storage-state JSON loading do not fix this.
- Volume concentration also flags: ~20 applications to one company in one cycle got 1 submitted / rest flagged. Spread per-company volume across days.

Practical rules for agent-browser sessions: use the launcher-prepared session as-is, fill the form correctly (see mechanics below), submit once, and read the result. A spam flag is **Blocked** — do not retry the same role in the same session. Flagged roles CAN be successfully resubmitted later from the user's live Chrome.

### Filling mechanics

Ashby's React custom controls do NOT register `agent-browser click`, coordinate clicks, or `press`. Use these:
- Scan every field wrapper with `document.querySelectorAll('[class*="fieldEntry_17tft"]')` — this catches radio/checkbox-group `<fieldset>` fields a `.ashby-application-form-field-entry` selector misses. Missing a required group field → submit fails "needs corrections".
- Text / textarea / file inputs: `agent-browser fill` and `agent-browser upload` work. System fields have stable ids `#_systemfield_name`, `#_systemfield_email`, `#_systemfield_resume`. Re-snapshot after a resume upload — refs shift.
- **Yes/No button questions:** backed by a hidden `<input type=checkbox>` whose `name` is the question UUID, with visible Yes/No buttons. Answer **Yes** → `check 'input[name="<uuid>"]'`. Answer **No** → `check` then `uncheck` that same input (a lone uncheck on an already-unchecked box is a no-op; you must end on a fired change event). Verify the chosen button gained a class containing `active`.
- **Real radios** (`<input type=radio>` — single acknowledgment radios and yes/no radio groups) and **multi-select checkbox groups:** `agent-browser ... check '<selector>'` works. Multi-select checkboxes are commonly keyed by `name="<option label>"`.
- **Combobox / location typeahead:** `type` the query into `input[role=combobox]`, wait for `[role=option]` to render, then commit the option via eval (a plain `.click()` will not commit it):
  ```js
  const o=[...document.querySelectorAll('[role=option]')].find(x=>x.textContent.trim()==='<value>');
  ['pointerdown','mousedown','pointerup','mouseup','click'].forEach(t=>o.dispatchEvent(new MouseEvent(t,{bubbles:true,cancelable:true,view:window})));
  ```
- **Submit:** eval `.click()` on the `<button>` whose text matches `/submit application/i`.
- Before submitting, re-scan every `[class*="fieldEntry_17tft"]` and confirm each required field is filled/answered. Submitting incomplete triggers a "needs corrections" round-trip = a second reCAPTCHA roll; avoid it.

### Reading the submit result (submit returns HTTP 200 even on failure — read the page text)

- "Your application was successfully submitted." → **Submitted**.
- Text contains "flagged as possible spam" → **Blocked** (reCAPTCHA score too low). Do not retry the same role this session.
- "needs corrections — Missing entry for required field: X" → fill X, resubmit once.
- "We couldn't submit your application" with wording about a per-candidate application limit, or a duplicate / reapply-cooldown message ("wait N months before reapplying") → **Blocked**, but note this is NOT a spam flag (reCAPTCHA passed; the company's own policy blocked it). The role is just un-submittable; move on.

### Honesty

Answer every question from the applicant profile, application policy, and resume. Do NOT answer technical-knowledge screening questions ("explain X", "what is Y", "write code for Z") or guess on export-control / "are you a US person" country-list questions — return Needs input for those.

## Workday And Custom ATS Notes

- Be cautious with account creation and multi-step applications.
- If the site requires login, persistent account setup, assessment, or detailed employment history not present in the resume, ask before continuing.
- Avoid saving credentials or making permanent account changes unless the user explicitly asks.

## Common Answer Policy

- Sponsorship: answer from `/Users/edwarddgao/openapply/config/applicant-profile.json`. For the current user, the persisted profile says sponsorship is not required now or in the future.
- Current authorization: answer by country from the persisted profile, not by desire to work there. For the current user, the persisted profile says current/legal authorization is `Yes` for the United States, Canada, and the United Kingdom.
- Relocation: answer yes only if the user has said they are willing to relocate or the role instructions already established that preference.
- Onsite/hybrid: answer yes only if the user has confirmed willingness for that location/schedule.
- Start date: use the user's known availability; if unknown and required, ask.
- Transcript: the current user does not have a transcript available. If a transcript upload is required and no transcript is available, re-upload the resume PDF rather than blocking.
- Desired salary: the current user's salary preference is negotiable. Use `Negotiable` for free-text salary fields. If a salary dropdown has no negotiable/equivalent option, choose a reasonable range overlapping the posted compensation range when available; otherwise ask.
- Years of experience: use full-time professional experience only if the question specifies full-time professional experience; do not count internships unless wording allows it.
- Education: use the resume's degree/school/graduation date. For the current user, McMaster University B.Eng. Software is `September 2019` to `May 2025` when a form requires month/year dates.
- Pronouns: if required and no decline option exists, use the user's known preference; otherwise ask.
- Negative screening questions: default `No` for required questions asking whether a risk, exception, restriction, conflict, violation, disciplinary/legal issue, debarment, sanctions exposure, politically exposed/government/state-owned affiliation, family relationship, non-compete, prior termination, or similar adverse condition applies to the applicant or immediate family unless the user has explicitly provided different information. If the question asks for an affirmative capability, commitment, consent, or factual preference instead, answer from known facts rather than defaulting.

## Blocker Log Format

When summarizing progress, use concise status lines:

- `Submitted: Company - Role - confirmation URL`
- `Blocked: Company - Role - exact blocker/error`
- `Needs input: Company - Role - specific question`

## Completion Criteria

- A role is complete only when the confirmation page or success response is verified.
- A role is blocked when a retry would require user action, a captcha/anti-spam challenge cannot be completed, the posting is unavailable, or a required answer is unknown.
- For batch work, continue to the next role after a confirmed submission or blocker unless the user asked to stop.
- All browser sessions created for the role must be closed before reporting final status.
