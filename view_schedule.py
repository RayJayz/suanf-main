#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查看排课结果
"""
import os
import pymysql
from collections import defaultdict
import sys
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from datetime import datetime


def view_schedule(version_id):
    """查看排课结果"""
    
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
        
        # 获取版本信息
        cursor.execute("SELECT * FROM schedule_versions WHERE version_id = %s", (version_id,))
        version = cursor.fetchone()
        
        if not version:
            print(f"错误：找不到版本ID为 {version_id} 的排课版本")
            return
        
        print("=" * 100)
        print(f"排课结果查看 - {version['version_name']} ({version['semester']})")
        print(f"状态: {version['status']}")
        print("=" * 100)
        
        # 获取排课结果
        query = """
        SELECT 
            sr.schedule_id,
            sr.week_day,
            sr.start_slot,
            sr.end_slot,
            tt.slots_count,
            c.course_name,
            co.course_nature,
            GROUP_CONCAT(DISTINCT t.teacher_name SEPARATOR ', ') AS teacher_name,
            cr.classroom_name,
            cr.capacity,
            co.student_count_estimate
        FROM schedules sr
        JOIN teaching_tasks tt ON sr.task_id = tt.task_id
        JOIN course_offerings co ON tt.offering_id = co.offering_id
        JOIN courses c ON co.course_id = c.course_id
        LEFT JOIN offering_teachers ot ON co.offering_id = ot.offering_id
        LEFT JOIN teachers t ON ot.teacher_id = t.teacher_id
        JOIN classrooms cr ON sr.classroom_id = cr.classroom_id
        WHERE sr.version_id = %s
        GROUP BY sr.schedule_id, sr.week_day, sr.start_slot, sr.end_slot, 
                 tt.slots_count, c.course_name, co.course_nature, 
                 cr.classroom_name, cr.capacity, co.student_count_estimate
        ORDER BY sr.week_day, sr.start_slot
        """
        
        cursor.execute(query, (version_id,))
        results = cursor.fetchall()
        
        if not results:
            print("该版本还没有排课结果")
            return
        
        print(f"\n共有 {len(results)} 条排课记录\n")
        
        # 按星期分组显示
        day_names = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
        schedule_by_day = defaultdict(list)
        
        for result in results:
            schedule_by_day[result['week_day']].append(result)
        
        # 显示每天的课表
        for day in range(1, 8):
            if day not in schedule_by_day:
                continue
            
            print(f"\n{'='*100}")
            print(f"{day_names[day]}")
            print(f"{'='*100}")
            print(f"{'节次':<6} {'课程名称':<25} {'性质':<8} {'教师':<12} {'教室':<15} {'人数/容量':<12}")
            print("-" * 100)
            
            for item in schedule_by_day[day]:
                slots_str = f"{item['start_slot']}-{item['end_slot']}"
                
                student_count = item['student_count_estimate'] or 0
                capacity = item['capacity'] or 0
                utilization = f"{student_count}/{capacity}"
                
                teacher_name = item['teacher_name'] or '未分配'
                
                print(f"{slots_str:<6} {item['course_name']:<25} {item['course_nature']:<8} "
                      f"{teacher_name:<12} {item['classroom_name']:<15} {utilization:<12}")
        
        # 统计信息
        print(f"\n{'='*100}")
        print("统计信息")
        print(f"{'='*100}")
        
        # 按课程性质统计
        nature_count = defaultdict(int)
        for result in results:
            nature_count[result['course_nature']] += 1
        
        print("\n课程性质分布:")
        for nature, count in nature_count.items():
            print(f"  {nature}: {count} 门")
        
        # 按星期统计
        print("\n每日课程数量:")
        for day in range(1, 8):
            if day in schedule_by_day:
                print(f"  {day_names[day]}: {len(schedule_by_day[day])} 门课程")
        
        # 教室利用率
        total_utilization = 0
        count = 0
        for result in results:
            if result['capacity'] and result['capacity'] > 0 and result['student_count_estimate']:
                util = result['student_count_estimate'] / result['capacity']
                total_utilization += util
                count += 1
        
        if count > 0:
            avg_utilization = total_utilization / count
            print(f"\n平均教室利用率: {avg_utilization:.1%}")
        
        print("\n" + "=" * 100)
        
        # 导出到 Excel
        export_to_excel(version, results, schedule_by_day)
        
    finally:
        cursor.close()
        conn.close()


def export_to_excel(version, results, schedule_by_day):
    """导出排课结果到 Excel"""
    
    # 创建工作簿
    wb = Workbook()
    
    # 删除默认工作表
    wb.remove(wb.active)
    
    # 样式定义
    header_font = Font(bold=True, size=11, color="FFFFFF")
    header_fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    day_names = ['', '周一', '周二', '周三', '周四', '周五', '周六', '周日']
    
    # 为每一天创建一个工作表
    for day in range(1, 8):
        if day not in schedule_by_day:
            continue
        
        ws = wb.create_sheet(title=day_names[day])
        
        # 设置列宽
        ws.column_dimensions['A'].width = 10
        ws.column_dimensions['B'].width = 30
        ws.column_dimensions['C'].width = 10
        ws.column_dimensions['D'].width = 15
        ws.column_dimensions['E'].width = 20
        ws.column_dimensions['F'].width = 15
        
        # 写入标题
        headers = ['节次', '课程名称', '性质', '教师', '教室', '人数/容量']
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = border
        
        # 写入数据
        for row_idx, item in enumerate(schedule_by_day[day], 2):
            slots_str = f"{item['start_slot']}-{item['end_slot']}"
            student_count = item['student_count_estimate'] or 0
            capacity = item['capacity'] or 0
            utilization = f"{student_count}/{capacity}"
            teacher_name = item['teacher_name'] or '未分配'
            
            data = [
                slots_str,
                item['course_name'],
                item['course_nature'],
                teacher_name,
                item['classroom_name'],
                utilization
            ]
            
            for col, value in enumerate(data, 1):
                cell = ws.cell(row=row_idx, column=col, value=value)
                cell.border = border
                cell.alignment = Alignment(horizontal="left", vertical="center")
    
    # 创建汇总表
    summary_ws = wb.create_sheet(title="汇总", index=0)
    summary_ws.column_dimensions['A'].width = 20
    summary_ws.column_dimensions['B'].width = 30
    
    # 写入版本信息
    summary_ws['A1'] = '排课版本'
    summary_ws['B1'] = version['version_name']
    summary_ws['A2'] = '学期'
    summary_ws['B2'] = version['semester']
    summary_ws['A3'] = '状态'
    summary_ws['B3'] = version['status']
    summary_ws['A4'] = '总课程数'
    summary_ws['B4'] = len(results)
    summary_ws['A5'] = '导出时间'
    summary_ws['B5'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 应用样式到汇总表
    for row in range(1, 6):
        summary_ws.cell(row=row, column=1).font = Font(bold=True)
    
    # 课程性质统计
    summary_ws['A7'] = '课程性质分布'
    summary_ws['A7'].font = Font(bold=True, size=12)
    
    nature_count = defaultdict(int)
    for result in results:
        nature_count[result['course_nature']] += 1
    
    row = 8
    for nature, count in nature_count.items():
        summary_ws.cell(row=row, column=1, value=nature)
        summary_ws.cell(row=row, column=2, value=f"{count} 门")
        row += 1
    
    # 每日课程数统计
    summary_ws.cell(row=row+1, column=1, value='每日课程数').font = Font(bold=True, size=12)
    row += 2
    for day in range(1, 8):
        if day in schedule_by_day:
            summary_ws.cell(row=row, column=1, value=day_names[day])
            summary_ws.cell(row=row, column=2, value=f"{len(schedule_by_day[day])} 门课程")
            row += 1
    
    # 保存文件
    filename = f"排课结果_版本{version['version_id']}_{version['semester']}.xlsx"
    wb.save(filename)
    
    print(f"\n✓ 排课结果已导出到: {filename}")
    print(f"  文件位置: {os.path.abspath(filename)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python view_schedule.py <version_id>")
        print("示例: python view_schedule.py 1")
        sys.exit(1)
    
    version_id = int(sys.argv[1])
    view_schedule(version_id)
