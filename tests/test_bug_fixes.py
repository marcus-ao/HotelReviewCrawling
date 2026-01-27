"""Bug修复验证测试脚本

测试所有P0级别Bug的修复是否成功
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def test_bug1_database_connection():
    """测试Bug #1: 数据库连接SQL语法错误修复"""
    print("\n" + "="*60)
    print("测试Bug #1: 数据库连接SQL语法错误")
    print("="*60)
    
    try:
        from database.connection import check_connection
        
        # 尝试连接数据库
        result = check_connection()
        
        if result:
            print("✅ Bug #1修复成功: 数据库连接正常")
            return True
        else:
            print("⚠️  数据库连接失败（可能是数据库未启动）")
            print("   请确保PostgreSQL正在运行")
            return False
            
    except Exception as e:
        print(f"❌ Bug #1修复失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_bug2_task_scheduler_query():
    """测试Bug #2: 任务调度器查询语法错误修复"""
    print("\n" + "="*60)
    print("测试Bug #2: 任务调度器查询语法错误")
    print("="*60)
    
    try:
        from scheduler import TaskScheduler
        from database.connection import check_connection
        
        # 检查数据库连接
        if not check_connection():
            print("⚠️  跳过测试: 数据库未连接")
            return False
        
        # 创建调度器实例
        scheduler = TaskScheduler()
        
        # 尝试创建评论任务（这会触发查询语法）
        # 注意：如果数据库中没有酒店数据，会返回空列表
        task_ids = scheduler.create_review_tasks()
        
        print(f"✅ Bug #2修复成功: 任务调度器查询正常")
        print(f"   创建了 {len(task_ids)} 个任务")
        return True
        
    except Exception as e:
        print(f"❌ Bug #2修复失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_bug3_review_id_generation():
    """测试Bug #3: 评论ID生成可能重复修复"""
    print("\n" + "="*60)
    print("测试Bug #3: 评论ID生成可能重复")
    print("="*60)
    
    try:
        from crawler import ReviewCrawler
        
        # 创建爬虫实例
        crawler = ReviewCrawler()
        
        # 测试生成1000个不同的review_id
        test_count = 1000
        ids = set()
        
        for i in range(test_count):
            review_id = crawler._generate_review_id(
                hotel_id="test_hotel",
                content=f"这是测试评论内容 {i}",
                user_nick=f"user_{i % 10}"  # 10个不同用户
            )
            ids.add(review_id)
        
        # 检查唯一性
        if len(ids) == test_count:
            print(f"✅ Bug #3修复成功: 生成了 {test_count} 个唯一ID")
            print(f"   示例ID: {list(ids)[:3]}")
            return True
        else:
            print(f"❌ Bug #3修复失败: 有重复ID")
            print(f"   生成数量: {test_count}, 唯一数量: {len(ids)}")
            print(f"   重复数量: {test_count - len(ids)}")
            return False
            
    except Exception as e:
        print(f"❌ Bug #3修复失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_review_id_consistency():
    """测试评论ID生成的一致性"""
    print("\n" + "="*60)
    print("额外测试: 评论ID生成一致性")
    print("="*60)
    
    try:
        from crawler import ReviewCrawler
        
        crawler = ReviewCrawler()
        
        # 相同输入应该生成相同ID
        id1 = crawler._generate_review_id("hotel1", "相同内容", "user1")
        id2 = crawler._generate_review_id("hotel1", "相同内容", "user1")
        
        if id1 == id2:
            print("✅ 相同输入生成相同ID（一致性良好）")
        else:
            print("❌ 相同输入生成不同ID（一致性问题）")
            return False
        
        # 不同输入应该生成不同ID
        id3 = crawler._generate_review_id("hotel1", "不同内容", "user1")
        id4 = crawler._generate_review_id("hotel2", "相同内容", "user1")
        id5 = crawler._generate_review_id("hotel1", "相同内容", "user2")
        
        if id1 != id3 and id1 != id4 and id1 != id5:
            print("✅ 不同输入生成不同ID（区分度良好）")
            return True
        else:
            print("❌ 不同输入生成相同ID（区分度问题）")
            return False
            
    except Exception as e:
        print(f"❌ 一致性测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("\n" + "="*60)
    print("P0级别Bug修复验证测试")
    print("="*60)
    print("\n开始测试...")
    
    results = {
        "Bug #1 (数据库连接)": test_bug1_database_connection(),
        "Bug #2 (任务调度器)": test_bug2_task_scheduler_query(),
        "Bug #3 (评论ID生成)": test_bug3_review_id_generation(),
        "额外测试 (ID一致性)": test_review_id_consistency(),
    }
    
    # 打印测试总结
    print("\n" + "="*60)
    print("测试总结")
    print("="*60)
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ 通过" if result else "❌ 失败"
        print(f"{status} - {test_name}")
    
    print("\n" + "-"*60)
    print(f"总计: {passed}/{total} 个测试通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！P0级别Bug已全部修复！")
        return 0
    else:
        print(f"\n⚠️  有 {total - passed} 个测试失败，请检查修复")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
