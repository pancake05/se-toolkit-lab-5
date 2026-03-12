"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func
from sqlalchemy import case, Date
from datetime import date
from typing import List, Dict, Any

from app.database import get_session
from app.models.item import ItemRecord
from app.models.interaction import InteractionLog
from app.models.learner import Learner
from app.auth import verify_api_key  # Функция проверки API ключа

router = APIRouter(dependencies=[Depends(verify_api_key)])  # Защищаем все эндпоинты


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Score distribution histogram for a given lab.
    
    Returns buckets: 0-25, 26-50, 51-75, 76-100 with counts.
    """
    # 1. Находим лабораторную работу по title
    # Предполагаем, что в БД title хранится как "Lab 04" для lab-04
    lab_title = lab.replace("-", " ")  # "lab-04" -> "lab 04"
    lab_title = lab_title.replace("lab", "Lab")  # "lab 04" -> "Lab 04"
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lab {lab} not found"
        )
    
    # 2. Находим все задания этой лабораторной
    statement = select(ItemRecord).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    tasks = result.scalars().all()
    task_ids = [task.id for task in tasks]
    
    if not task_ids:
        # Если нет заданий, возвращаем пустые бакеты
        return [
            {"bucket": "0-25", "count": 0},
            {"bucket": "26-50", "count": 0},
            {"bucket": "51-75", "count": 0},
            {"bucket": "76-100", "count": 0}
        ]
    
    # 3. Группируем оценки по бакетам
    # Используем CASE WHEN для создания бакетов
    buckets = [
        case(
            (InteractionLog.score <= 25, "0-25"),
            (InteractionLog.score <= 50, "26-50"),
            (InteractionLog.score <= 75, "51-75"),
            else_="76-100"
        ).label("bucket"),
        func.count().label("count")
    ]
    
    statement = select(*buckets).where(
        InteractionLog.item_id.in_(task_ids),
        InteractionLog.score.isnot(None)  # Только взаимодействия с оценкой
    ).group_by("bucket")
    
    result = await session.execute(statement)
    rows = result.all()
    
    # 4. Преобразуем в словарь для удобства
    counts_by_bucket = {row.bucket: row.count for row in rows}
    
    # 5. Всегда возвращаем все 4 бакета (даже с 0)
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
    # 1. Находим лабораторную работу
    lab_title = lab.replace("-", " ").replace("lab", "Lab")
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lab {lab} not found"
        )
    
    # 2. Находим все задания и их статистику
    # Используем JOIN для получения данных за один запрос
    statement = select(
        ItemRecord.title.label("task"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(InteractionLog.id).label("attempts")
    ).join(
        InteractionLog, InteractionLog.item_id == ItemRecord.id, isouter=True
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
    
    # 3. Форматируем результат
    return [
        {
            "task": row.task,
            "avg_score": float(row.avg_score) if row.avg_score else 0.0,
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
    # 1. Находим лабораторную работу
    lab_title = lab.replace("-", " ").replace("lab", "Lab")
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lab {lab} not found"
        )
    
    # 2. Находим все задания этой лабораторной
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    task_ids = result.scalars().all()
    
    if not task_ids:
        return []
    
    # 3. Группируем по дате
    statement = select(
        func.cast(InteractionLog.created_at, Date).label("date"),
        func.count().label("submissions")
    ).where(
        InteractionLog.item_id.in_(task_ids)
    ).group_by(
        func.cast(InteractionLog.created_at, Date)
    ).order_by(
        "date"
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    # 4. Форматируем результат
    return [
        {
            "date": row.date.isoformat(),
            "submissions": row.submissions
        }
        for row in rows
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
) -> List[Dict[str, Any]]:
    """Per-group performance for a given lab."""
    # 1. Находим лабораторную работу
    lab_title = lab.replace("-", " ").replace("lab", "Lab")
    
    statement = select(ItemRecord).where(
        ItemRecord.type == "lab",
        ItemRecord.title.contains(lab_title)
    )
    result = await session.execute(statement)
    lab_item = result.scalar_one_or_none()
    
    if not lab_item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Lab {lab} not found"
        )
    
    # 2. Находим все задания этой лабораторной
    statement = select(ItemRecord.id).where(
        ItemRecord.type == "task",
        ItemRecord.parent_id == lab_item.id
    )
    result = await session.execute(statement)
    task_ids = result.scalars().all()
    
    if not task_ids:
        return []
    
    # 3. Группируем по группам студентов
    statement = select(
        Learner.student_group.label("group"),
        func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
        func.count(func.distinct(Learner.id)).label("students")
    ).join(
        InteractionLog, InteractionLog.learner_id == Learner.id
    ).where(
        InteractionLog.item_id.in_(task_ids),
        Learner.student_group.isnot(None)  # Только студенты с указанной группой
    ).group_by(
        Learner.student_group
    ).order_by(
        Learner.student_group
    )
    
    result = await session.execute(statement)
    rows = result.all()
    
    # 4. Форматируем результат
    return [
        {
            "group": row.group,
            "avg_score": float(row.avg_score) if row.avg_score else 0.0,
            "students": row.students
        }
        for row in rows
    ]
