#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分析排课结果中的冲突问题
"""
import os
import pymysql
from collections import defaultdict


def analyze_schedule_conflicts(version_id):
    """分析指定版本的排课冲突"""
    
    # 连接数据库
    conn = pymysql.connect(
        host=os.getenv("DB_HOST") or "localhost",
        port=int(os.getenv("DB_PORT") or "3306"),
        user=os.getenv("DB_USER") or "root",
        password=os.getenv("DB_PASSWORD") or "20051113Da",
        database=os.getenv("DB_NAME") or "paike2",
        charset="utf8mb4",
    )
    
    try:
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 获取排课结果
        query = """
        SELECT 
            sr.schedule_id,
            sr.task_id,
            sr.teacher_id,
            sr.classroom_id,
            sr.week_day,
            sr.start_slot,
            tt.slots_count,
            tt.offering_id,
            co.course_name,
            t.teacher_name,
            cr.classroom_name
        FROM schedule_results sr
        JOIN teaching_tasks tt ON sr.task_id = tt.task_id
        JOIN course_offerings co ON tt.offering_id = co.offering_id
        JOIN teachers t ON sr.teacher_id = t.teacher_id
        JOIN classrooms cr ON sr.classroom_id = cr.classroom_id
        WHERE sr.version_id = %s
        ORDER BY sr.week_day, sr.start_slot
        """
        
        cursor.execute(query, (version_id,))
        results = cursor.fetchall()
        
        # 获取任务-班级关系
        task_classes = defaultdict(list)
        for result in results:
            task_id = result['task_id']
            class_query = """
            SELECT c.class_id, c.class_name
            FROM task_classes tc
            JOIN classes c ON tc.class_id = c.class_id
            WHERE tc.task_id = %s
            """
            cursor.execute(class_query, (task_id,))
            classes = cursor.fetchall()
            task_classes[task_id] = classes
        
        print("=" * 80)
        print(f"排课版本 {version_id} 冲突分析报告")
        print("=" * 80)
        
        # 分析班级冲突
        print("\n【班级时间冲突分析】")
        class_schedule = defaultdict(list)
        
        for result in results:
            task_id = result['task_id']
            start_slot = result['start_slot']
            end_slot = start_slot + result['slots_count'] - 1
            week_day = result['week_day']
            
            for cls in task_classes[task_id]:
                class_id = cls['class_id']
                for slot in range(start_slot, end_slot + 1):
                    time_key = (week_day, slot)
                    class_schedule[class_id].append({
                        'time': time_key,
                        'course': result['course_name'],
                        'teacher': result['teacher_name'],
                        'classroom': result['classroom_name'],
                        'class_name': cls['class_name']
                    })
        
        # 检测冲突
        conflicts_found = 0
        for class_id, schedule in class_schedule.items():
            time_dict = defaultdict(list)
            for item in schedule:
                time_dict[item['time']].append(item)
            
            for time, items in time_dict.items():
                if len(items) > 1:
                    conflicts_found += 1
                    week_day, slot = time
                    day_names = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
                    print(f"\n冲突 #{conflicts_found}:")
                    print(f"  班级: {items[0]['class_name']}")
                    print(f"  时间: {day_names[week_day]} 第{slot}节")
                    for i, item in enumerate(items, 1):
                        print(f"  课程{i}: {item['course']} - {item['teacher']} - {item['classroom']}")
        
        if conflicts_found == 0:
            print("✓ 未发现班级冲突")
        else:
            print(f"\n⚠ 共发现 {conflicts_found} 处班级冲突")
        
        # 分析教师冲突
        print("\n【教师时间冲突分析】")
        teacher_schedule = defaultdict(list)
        
        for result in results:
            teacher_id = result['teacher_id']
            start_slot = result['start_slot']
            end_slot = start_slot + result['slots_count'] - 1
            week_day = result['week_day']
            
            for slot in range(start_slot, end_slot + 1):
                time_key = (week_day, slot)
                teacher_schedule[teacher_id].append({
                    'time': time_key,
                    'course': result['course_name'],
                    'teacher': result['teacher_name'],
                    'classroom': result['classroom_name']
                })
        
        teacher_conflicts = 0
        for teacher_id, schedule in teacher_schedule.items():
            time_dict = defaultdict(list)
            for item in schedule:
                time_dict[item['time']].append(item)
            
            for time, items in time_dict.items():
                if len(items) > 1:
                    teacher_conflicts += 1
                    week_day, slot = time
                    day_names = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
                    print(f"\n冲突 #{teacher_conflicts}:")
                    print(f"  教师: {items[0]['teacher']}")
                    print(f"  时间: {day_names[week_day]} 第{slot}节")
                    for i, item in enumerate(items, 1):
                        print(f"  课程{i}: {item['course']} - {item['classroom']}")
        
        if teacher_conflicts == 0:
            print("✓ 未发现教师冲突")
        else:
            print(f"\n⚠ 共发现 {teacher_conflicts} 处教师冲突")
        
        # 分析教室冲突
        print("\n【教室时间冲突分析】")
        classroom_schedule = defaultdict(list)
        
        for result in results:
            classroom_id = result['classroom_id']
            start_slot = result['start_slot']
            end_slot = start_slot + result['slots_count'] - 1
            week_day = result['week_day']
            
            for slot in range(start_slot, end_slot + 1):
                time_key = (week_day, slot)
                classroom_schedule[classroom_id].append({
                    'time': time_key,
                    'course': result['course_name'],
                    'teacher': result['teacher_name'],
                    'classroom': result['classroom_name']
                })
        
        classroom_conflicts = 0
        for classroom_id, schedule in classroom_schedule.items():
            time_dict = defaultdict(list)
            for item in schedule:
                time_dict[item['time']].append(item)
            
            for time, items in time_dict.items():
                if len(items) > 1:
                    classroom_conflicts += 1
                    week_day, slot = time
                    day_names = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
                    print(f"\n冲突 #{classroom_conflicts}:")
                    print(f"  教室: {items[0]['classroom']}")
                    print(f"  时间: {day_names[week_day]} 第{slot}节")
                    for i, item in enumerate(items, 1):
                        print(f"  课程{i}: {item['course']} - {item['teacher']}")
        
        if classroom_conflicts == 0:
            print("✓ 未发现教室冲突")
        else:
            print(f"\n⚠ 共发现 {classroom_conflicts} 处教室冲突")
        
        print("\n" + "=" * 80)
        print("分析完成")
        print("=" * 80)
        
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("用法: python analyze_conflicts.py <version_id>")
        print("示例: python analyze_conflicts.py 1")
        sys.exit(1)
    
    version_id = int(sys.argv[1])
    analyze_schedule_conflicts(version_id)
