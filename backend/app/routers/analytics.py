"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from sqlalchemy import case, Date
from typing import List, Dict, Any
from datetime import datetime

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner

router = APIRouter()


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Score distribution histogram for a given lab.
    
    Returns buckets: 0-25, 26-50, 51-75, 76-100 with counts.
    """
    # 1. Находим лабораторную работу по title
    lab_num = lab.split('-')[-1]
    lab_title_pattern = f"Lab {lab_num}"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title_pattern)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # 2. Находим все задания этой лабораторной
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    task_ids = result.scalars().all()
    
    if not task_ids:
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # 3. Группируем оценки по бакетам
    bucket_case = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100"
    ).label("bucket")
    
    statement = select(
        bucket_case,
        func.count().label("count")
    ).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)
    ).group_by(
        "bucket"
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    counts_by_bucket = {row.bucket: row.count for row in rows}
    
    return [
        {"bucket": "0-25", "count": counts_by_bucket.get("0-25", 0)},
        {"bucket": "26-50", "count": counts_by_bucket.get("26-50", 0)},
        {"bucket": "51-75", "count": counts_by_bucket.get("51-75", 0)},
        {"bucket": "76-100", "count": counts_by_bucket.get("76-100", 0)}
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Per-task pass rates for a given lab."""
    lab_num = lab.split('-')[-1]
    lab_title_pattern = f"Lab {lab_num}"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title_pattern)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    statement = select(
        ItemRecord.title.label("task"),
        func.coalesce(func.round(func.avg(InteractionLog.score), 1), 0.0).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).outerjoin(
        InteractionLog, 
        (InteractionLog.item_id == ItemRecord.id) & (InteractionLog.score.isnot(None))
    ).where(
        ItemRecord.parent_id == lab_item.id,
        ItemRecord.type == "task"
    ).group_by(
        ItemRecord.id, ItemRecord.title
    ).order_by(
        ItemRecord.title
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score),
            "attempts": row.attempts
        }
        for row in rows
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Submissions per day for a given lab."""
    lab_num = lab.split('-')[-1]
    lab_title_pattern = f"Lab {lab_num}"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title_pattern)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    task_ids = result.scalars().all()
    
    if not task_ids:
        return []
    
    # Используем func.date для группировки по дате
    statement = select(
        func.date(InteractionLog.created_at).label("date"),
        func.count().label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.date(InteractionLog.created_at)
    ).order_by(
        func.date(InteractionLog.created_at)
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    # Преобразуем дату в строку, проверяя тип
    formatted_result = []
    for row in rows:
        date_value = row.date
        # Если это строка, используем как есть, если datetime - конвертируем
        if isinstance(date_value, str):
            date_str = date_value
        else:
            date_str = date_value.isoformat() if hasattr(date_value, 'isoformat') else str(date_value)
        
        formatted_result.append({
            "date": date_str,
            "submissions": row.submissions
        })
    
    return formatted_result


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Per-group performance for a given lab."""
    lab_num = lab.split('-')[-1]
    lab_title_pattern = f"Lab {lab_num}"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title_pattern)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        return []
    
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    task_ids = result.scalars().all()
    
    if not task_ids:
        return []
    
    statement = select(
        Learner.student_group.label("group"),
        func.coalesce(func.round(func.avg(InteractionLog.score), 1), 0.0).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        Learner.student_group.isnot(None),
        InteractionLog.score.isnot(None)
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score),
            "students": row.students
        }
        for row in rows
    ]
