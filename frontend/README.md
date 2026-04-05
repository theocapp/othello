# Othello frontend

This frontend is a Vite + React client for the Othello backend API.

## Local development

```bash
cd frontend
npm install
npm run dev
```

By default, local development targets `http://127.0.0.1:8001`.

## Environment

Create a `.env` file from `.env.example` when you need an explicit API origin.

```bash
cp .env.example .env
```

`VITE_API_BASE_URL`
- Leave blank in local development to use the built-in localhost default.
- Set it in production if your API is hosted on a different origin than the frontend.
- If your production frontend and API share the same origin, leave it unset so requests stay same-origin.

## Production build

```bash
cd frontend
npm install
npm run build
npm run preview
```

## Common failure modes

- `Network Error`: the backend is not running, or `VITE_API_BASE_URL` points to the wrong place.
- Blank data panels: the frontend loaded, but the backend endpoints are failing.
- CORS errors: update `OTHELLO_CORS_ORIGINS` in the backend environment.
