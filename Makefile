backend:
	cd backend && uvicorn main:app --reload --port 8001

worker:
	cd backend && python worker.py

frontend:
	cd frontend && npm run dev

test:
	cd backend && python -m unittest discover -p "test_*.py" -v
	cd frontend && npm run build
