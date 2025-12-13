# audio-submission-processor

Processes Google Form submission rows from a Google Sheet and:
- Downloads the submitted audio file from **My Drive**
- Applies in-file tags using **mutagen** (best-effort for supported formats)
- Uploads the modified file into a **Shared Drive** destination folder structure:
  - `DEST_ROOT_FOLDER_ID / <DivisionSubfolder> / <FinalFilename>`
- Deletes the original file
- Marks the submission row as processed (`X`) in the **last column** of the sheet

## Filename rules

Final filename:
- Prefix: `LeaderFirstLeaderLast_FollowerFirstFollowerLast_Division_`
- Tail always includes a **season year** `YYYY`:
  - if submission month is **November (11) or December (12)**, season year is **next year**
  - else it is the submission year
- If `Routine Name` and/or `Personal Descriptor` are present, append them after the year:
  - `YYYY_RoutineName_PersonalDescriptor` (whichever are present, in that order)
- Always includes a version suffix:
  - start with `_v1`
  - if a collision exists in the destination folder, increment to `_v2`, `_v3`, ...

Example:
- `KaianoLevine_LibbyWooton_NoviceJack_Jill_2025_WestCoastSwing_SparklyShoes_v1.mp3`

## Tagging rules (mutagen)

Before writing new tags:
- Read any existing **Title**, **Artist**, **Album**, and **Comment**
- Concatenate any non-empty values with ` | `
- Store that concatenation into **Comment**

Then set:
- **Title**: `LeaderFirstLeaderLast & FollowerFirstFollowerLast`
- **Artist**: `Division YYYY, RoutineName, Personal Descriptor`
  - Always includes `Division YYYY`
  - Routine/Descriptor are appended if present

Album is left unchanged.

If mutagen cannot tag a file type, the file is still uploaded (bytes unchanged), and processing continues.

## Sheet processing rule

- The submission sheet has **11 fixed input columns** (positional; header text is ignored).
- The **processed flag is the last column**.
- If there are only 11 columns, the processor adds one extra column.
- A row is considered processed if the last cell is `X`.
- The processor writes `X` only after upload AND delete succeed.

## Setup

### 1) Install

This repo uses Poetry.

```bash
poetry install
```

### 2) Google credentials

Create a Google Cloud **service account** and download its JSON key.

Share with that service account:
- the **submission spreadsheet**
- the **destination shared drive folder** (and/or shared drive)
- ensure it has access to the personal-drive files being submitted (typically via shared link permission)

Set:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/absolute/path/to/service-account.json"
```

### 3) Configure environment variables

Required:

```bash
export SUBMISSION_SHEET_ID="your-spreadsheet-id"
export SUBMISSIONS_FOLDER_ID="all-submissions-folder-id"
```

Optional:

```bash
export DEST_ROOT_FOLDER_ID="root-folder-id-for-division-subfolders"
export WORKSHEET_NAME="Form Responses 1"   # defaults to first sheet
export LOG_LEVEL="INFO"
```

Notes:
- If `DEST_ROOT_FOLDER_ID` is not set, division folders are created under `SUBMISSIONS_FOLDER_ID`.

## Run

```bash
poetry run python -m audio_submission_processor.main
```

## Development

### Tests

A minimal test suite can be added later; this repo is structured to keep most logic pure and unit-testable.

### Common gotchas

- If the processor keeps retrying a row, the Drive steps are failing. Check logs.
- Drive file IDs are extracted with a regex; ensure the sheet's audio URL includes a Drive link or file ID.
- Shared Drive operations require `supportsAllDrives=True` which is enabled in the code.

## License

MIT (add/adjust as needed).
