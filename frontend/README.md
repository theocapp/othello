# Frontend quickstart

Minimal instructions to get the developer UI running locally.

Prerequisites:
- Node 18+ and npm or yarn
- Backend running at `http://127.0.0.1:8001` (default)

Dev server:

```bash
cd frontend
npm install
npm run dev
```

The app uses `VITE_API_BASE_URL` when set; otherwise local dev targets `http://127.0.0.1:8001`.

Running a production preview:

```bash
npm run build
npm run preview
```

Folder notes:
- `src/` — React UI. We favor small feature hooks under `src/hooks/` and minimal data-layer code in `src/api.js`.
- `public/` — static assets

Contributor tips (2-minute setup):
- Start the backend: `cd backend && ./run_backend.sh` (or `uvicorn main:app --reload`)
- Start frontend dev: `cd frontend && npm run dev`
- Open `http://localhost:5173`

If you run into API errors, confirm `VITE_API_BASE_URL` and that the backend is reachable.

