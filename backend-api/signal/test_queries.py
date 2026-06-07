"""
快速测试脚本 - 在启动服务器前验证数据库查询是否正常
运行方式：python test_queries.py
"""
from data_layer import calc_volume_trend, calc_mev_toxicity, get_available_tokens

def test_all():
    print("=" * 50)
    print("🧪 信号参谋 - 数据层测试")
    print("=" * 50)

    # 1. 看数据库里有哪些代币
    print("\n📋 Step 1: 查询数据库中的代币列表...")
    try:
        tokens = get_available_tokens()
        print(f"✅ 找到 {len(tokens)} 个代币: {tokens[:10]}")
        test_token = tokens[0] if tokens else "USDT"
    except Exception as e:
        print(f"❌ 失败: {e}")
        test_token = "USDT"

    print(f"\n🔍 使用 [{test_token}] 进行后续测试")

    # 2. 测试成交量趋势
    print(f"\n📊 Step 2: 计算 {test_token} 成交量趋势...")
    try:
        vt = calc_volume_trend(test_token)
        print(f"✅ 近24h成交量: ${vt.volume_24h:,.2f}")
        print(f"   近7日日均:   ${vt.volume_7d_avg:,.2f}")
        print(f"   量比:        {vt.ratio:.2f}x")
        print(f"   趋势标签:    {vt.get_trend_label()}")
        print(f"   得分:        {vt.score}/15")
        print(f"   近24h笔数:   {vt.data_points_24h}笔")
    except Exception as e:
        print(f"❌ 失败: {e}")

    # 3. 测试MEV污染度
    print(f"\n🤖 Step 3: 计算 {test_token} MEV污染度...")
    try:
        mev = calc_mev_toxicity(test_token)
        print(f"✅ 总交易笔数:   {mev.total_txs}")
        print(f"   疑似MEV:     {mev.suspicious_txs}")
        print(f"   污染度:      {mev.toxicity_pct:.2f}%")
        print(f"   污染标签:    {mev.get_toxicity_label()}")
        print(f"   得分:        {mev.score}/25")
    except Exception as e:
        print(f"❌ 失败: {e}")

    print("\n" + "=" * 50)
    print("测试完成！如果以上都是 ✅，可以启动服务器了：")
    print("  python main.py")
    print("  访问 http://localhost:8000/signal/USDT 查看效果")
    print("=" * 50)


if __name__ == "__main__":
    test_all()