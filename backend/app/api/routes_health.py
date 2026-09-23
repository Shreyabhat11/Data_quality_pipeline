from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health_check():
    """
    Liveness check. Deliberately does NOT touch the database -- a DB outage
    should not make the process look dead to a load balancer / uptime check.
    """
    return {"status": "ok"}
