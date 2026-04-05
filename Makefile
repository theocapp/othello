backend:
	cd backend && uvicorn main:app --reload --port 8001

worker:
	cd backend && python worker.py

frontend:
	cd frontend && npm run dev

test:
	cd backend && python -m unittest test_api_smoke -v
	cd frontend && npm run build
