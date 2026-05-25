# 实时反洗钱监控和报送系统 - 设计文档

## 概述

基于 Lambda 架构的实时反洗钱（AML）监控和报送系统，符合 FATF 国际标准。系统监控银行转账、卡交易和外汇交易，日处理规模 100M+ 笔，高风险告警目标 30 分钟内处理，汇总报告 T+1。

## 需求总结

| 维度 | 选择 |
|------|------|
| 监管体系 | FATF 国际标准 |
| 技术栈 | Java/Scala + Kafka + Spark Structured Streaming + Hive + HBase + ClickHouse |
| 交易类型 | 银行转账/汇款、卡交易、外汇交易 |
| 数据规模 | 100M+/天 |
| 核心能力 | 规则引擎、制裁名单筛查、KYC/CDD、用户特征工程 |
| 报送类型 | CTR、SAR/STR、定期汇总、监管 API 对接 |
| 时效要求 | 高风险 6h 内报送（目标 30min），汇总 T+1 |
| 数据源 | Kafka Topic |
| 告警流程 | 完整工作流：告警→审核→调查→报送 |
| 图分析 | 后续阶段扩展 |

## 架构：Lambda 架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        数据接入层                                    │
│  Kafka Topics: txn.bank / txn.card / txn.forex / entity.customer   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                    ┌──────┴──────┐
                    ▼             ▼
┌─────────────────────────┐ ┌─────────────────────────────────────────┐
│      实时层              │ │         批处理层                        │
│  Spark Structured       │ │  Spark on Hive                         │
│  Streaming              │ │  - 特征工程（用户画像）                 │
│  - 规则引擎              │ │  - KYC/CDD 批量评估                    │
│  - 制裁名单筛查          │ │  - 汇总报告生成                        │
│  - CTR 自动生成          │ │  - 模型训练/更新                       │
│  - SAR 模式检测          │ │  - 告警对账                            │
└────────────┬────────────┘ └────────────────┬────────────────────────┘
             │                               │
             ▼                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        服务层                                       │
│  ClickHouse: 告警查询 / 统计分析 / 报表                             │
│  HBase: 客户实体 / 交易画像 / KYC 状态                              │
│  Redis: 规则缓存 / 制裁名单 Bloom Filter / 会话状态                 │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        应用层                                       │
│  告警工作流引擎 / 案件管理 / 报告生成 / 监管 API 对接               │
└─────────────────────────────────────────────────────────────────────┘
```

## 实时处理链路（4 层 + 旁路）

### Layer 1: 数据标准化

- 多源交易数据统一为 Canonical Transaction Schema
- 字段映射、编码转换、币种标准化
- 数据质量校验（无效记录 → Dead Letter Queue）
- 按 `txn_id` 分区，保证均匀分布
- 输出: Kafka Topic `txn.normalized`

### Layer 2: 特征加载 & 窗口指标计算

- 从 HBase/Redis 加载客户画像（风险等级、KYC 状态、历史统计）
- 滑动窗口聚合: 1h/24h/7d 交易频次、金额累计、对手方数量
- 有状态流处理: 按 `customer_id` 分区
- 输出: Kafka Topic `txn.enriched`

### Layer 3: 规则引擎 & 制裁筛查

- CTR 检测: 单笔超阈值 → 自动生成 CTR 报告
- SAR 规则匹配: 拆分交易、频繁交易、异常模式
- 制裁名单筛查: Bloom Filter 快速过滤 + 精确模糊匹配
- 规则热加载: 从 DB/配置中心动态更新规则
- 输出: Kafka Topics `alert.raw` / `ctr.event` / `sanction.hit`

### Layer 4: 溯源分析 & 告警聚合

- 中高风险告警触发资金链路回溯（最近 N 跳）
- 关联分析: 同一客户/对手方的多笔告警聚合
- 告警去重 & 优先级排序
- 写入告警工作流引擎（ClickHouse + 业务系统）

### 旁路链路（防数据倾斜）

```
                    ┌───────────────────────────────┐
                    │  Layer 1: 数据标准化           │
                    │  (按 txn_id 均匀分区)          │
                    └───────────────┬───────────────┘
                          ┌─────────┴─────────┐
                          ▼                   ▼
              ┌───────────────────┐ ┌───────────────────┐
              │  主链路            │ │  旁路              │
              │  普通客户交易      │ │  热点客户/高频交易  │
              └─────────┬─────────┘ └─────────┬─────────┘
                        │                     │
                        ▼                     ▼
          ┌─────────────────────┐ ┌─────────────────────┐
          │  Layer 2a: 同步      │ │  Layer 2b: 异步      │
          │  HBase/Redis 查询   │ │  预计算特征           │
          │  窗口聚合           │ │  独立 Job             │
          └─────────┬───────────┘ └─────────┬───────────┘
                    └──────────┬────────────┘
                               ▼
                  ┌───────────────────────────┐
                  │  Layer 3: 规则引擎        │
                  │  无状态，水平扩展          │
                  └─────────────┬─────────────┘
                       ┌────────┴────────┐
                       ▼                 ▼
              ┌──────────────┐ ┌──────────────┐
              │  低风险告警   │ │  中高风险告警 │
              │  自动处置     │ │  触发溯源     │
              └──────────────┘ └──────┬───────┘
                                      ▼
                        ┌───────────────────────┐
                        │  Layer 4: 聚合        │
                        └───────────────────────┘
```

| 场景 | 主链路 | 旁路 |
|------|--------|------|
| 高频客户（日交易 >1000 笔） | 跳过窗口计算，从 Redis 读预计算特征 | 独立 Job 异步更新 Redis |
| 大额交易（超 CTR 阈值） | 直接跳到 Layer 3，跳过特征加载 | — |
| 制裁名单命中 | Layer 3 直接标红，优先处理 | — |
| 批量入账（同一对手方 >N 笔） | 按 batch_id 聚合后一次性处理 | — |

## 数据模型

### Canonical Transaction Model

| 字段 | 类型 | 描述 |
|------|------|------|
| txn_id | STRING | 全局唯一 ID，跨业务线 |
| txn_type | ENUM | TRANSFER / CARD / FOREX |
| timestamp | BIGINT | 毫秒级 UTC |
| amount | DECIMAL | 原始金额 |
| currency | STRING | ISO 4217 |
| amount_usd | DECIMAL | 标准化为 USD 等值 |
| direction | ENUM | INBOUND / OUTBOUND |
| customer_id | STRING | 发起方 |
| counterparty_id | STRING | 接收方 |
| channel | STRING | ATM / ONLINE / BRANCH / SWIFT |
| country_src | STRING | 发起国 ISO 3166 |
| country_dst | STRING | 接收国 ISO 3166 |
| metadata | MAP | 业务线特有字段 |
| raw_payload | STRING | 原始报文，用于审计 |

### 存储层职责分工

| 存储 | 用途 | 数据特征 | 保留策略 |
|------|------|----------|----------|
| **Kafka** | 实时管道 | 消息队列，不落盘 | 7 天 |
| **Hive** | 原始数据归档 + 批处理 | 分区表（按天），Parquet 格式 | 永久 |
| **HBase** | 实体数据 + 实时画像 | 客户实体、KYC 状态、风险评级 | 永久 |
| **ClickHouse** | 告警 + 分析查询 | 列存，支持 OLAP 查询 | 3 年 |
| **Redis** | 热缓存 | 规则缓存、制裁名单 Bloom Filter、窗口指标 | 按 TTL |

### Hive 表分区策略

```
/warehouse/aml/
├── txn_normalized/          # Layer 1 输出归档
│   └── dt=2026-05-25/
│       └── txn_type=BANK/
├── txn_enriched/            # Layer 2 输出归档
│   └── dt=2026-05-25/
├── alert_history/           # 告警历史归档
│   └── dt=2026-05-25/
├── entity_snapshot/         # 每日实体快照
│   └── dt=2026-05-25/
└── rule_versions/           # 规则版本快照
    └── dt=2026-05-25/
```

### ClickHouse 告警表

```sql
CREATE TABLE aml.alert_queue (
    alert_id        String,
    txn_id          String,
    customer_id     String,
    alert_type      Enum8('CTR'=1, 'SAR'=2, 'SANCTION'=3, 'RULE'=4),
    severity        Enum8('LOW'=1, 'MEDIUM'=2, 'HIGH'=3, 'CRITICAL'=4),
    rule_id         String,
    rule_desc       String,
    score           Float32,
    status          Enum8('NEW'=1, 'REVIEWING'=2, 'ESCALATED'=3, 'CLOSED'=4, 'REPORTED'=5),
    created_at      DateTime64(3),
    updated_at      DateTime64(3),
    reviewer_id     Nullable(String),
    notes           Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (severity, customer_id, created_at)
TTL created_at + INTERVAL 3 YEAR;
```

### HBase 客户实体表

```
RowKey: customer_id (hash 前缀 + customer_id，避免热点)

Column Families:
├── cf:profile        # 基础画像
│   ├── name, id_type, id_number, nationality
│   ├── occupation, income_source, address
│   └── kyc_level, kyc_expiry
├── cf:risk           # 风险评级
│   ├── risk_level (LOW/MEDIUM/HIGH/PROHIBITED)
│   ├── risk_score, risk_factors_json
│   └── last_review_date, next_review_date
├── cf:txn_stats      # 交易统计（由 Layer 2 更新）
│   ├── txn_count_7d, txn_count_30d
│   ├── total_amount_7d, total_amount_30d
│   └── avg_amount, max_amount, unique_counterparties_30d
└── cf:alert          # 告警历史
    ├── alert_count_total, alert_count_90d
    ├── last_alert_date, last_alert_type
    └── open_alert_count
```

## 规则引擎

### 架构

- 规则 DSL（YAML 格式）定义检测规则
- 配置驱动的轻量级规则执行引擎（不引入 Drools 等外部框架）
  - YAML 规则解析为内存条件树
  - 简单条件评估器：字段比较、阈值检查、集合成员判断
  - 组合规则通过 AND/OR/NOT 逻辑运算符
  - 基于窗口的条件通过 Layer 2 预计算的特征评估
- 规则版本管理，每个版本有 effective_from 时间戳
- 热加载: DB → Redis → Streaming（原子切换）
- 按交易时间匹配规则版本，而非处理时间

### 规则热加载 & 一致性保障

**规则版本时间线：**
- 每个规则版本有 `effective_from` 时间戳
- 查找逻辑: `SELECT * FROM rule_versions WHERE effective_from <= txn.timestamp ORDER BY effective_from DESC LIMIT 1`
- 实时和离线链路都按交易时间匹配规则版本

**热加载流程：**
1. 规则管理员创建/修改规则，设置未来的 `effective_from`
2. CDC/轮询检测 DB 变更
3. Redis 缓存更新: 规则快照 + 版本索引（sorted set by time）
4. Streaming Job 刷新或接收广播通知
5. 新规则版本加载到内存

**实时/离线一致性保障：**
1. 统一规则存储: Hive `aml.rule_versions` 表
2. 快照机制: 每次规则变更生成完整快照
3. 按交易时间判定: 实时读 Redis sorted set，离线读 Hive
4. 对账机制: T+1 离线任务对比实时告警结果，差异率目标 < 0.1%

### 预置规则

| 规则 ID | 类型 | 描述 | 触发条件 |
|---------|------|------|----------|
| CTR-001 | CTR | 大额现金交易 | 单笔 ≥ $10,000 且 channel IN (ATM, BRANCH) |
| SAR-001 | SAR | 拆分交易检测 | 24h 内: ≥5 笔，每笔 <$10,000，累计 ≥$40,000 |
| SAR-002 | SAR | 异常频繁交易 | 1h 内: 频次 ≥ 历史均值 ×5 |
| SAR-003 | SAR | 高风险国家转账 | country_dst ∈ 高风险国家列表 且 amount ≥$5,000 |
| SAR-004 | SAR | 快进快出 | 24h 内: 入账后立即转出，留存 <10% |
| SAN-001 | 制裁 | 制裁名单匹配 | 客户/对手方命中 OFAC/EU/UN 名单（模糊匹配 ≥85%） |

### 规则 DSL 示例

```yaml
rule:
  id: SAR-001
  name: "Structuring Detection"
  type: SAR
  severity: HIGH
  window: 24h
  conditions:
    - field: txn_count_24h
      operator: GTE
      value: 5
    - field: max_single_amount
      operator: LT
      value: 10000
    - field: total_amount_24h
      operator: GTE
      value: 40000
    - field: channel
      operator: IN
      value: [ATM, BRANCH, ONLINE]
  actions:
    - type: ALERT
      template: "Potential structuring: {txn_count_24h} transactions totaling ${total_amount_24h}"
    - type: ESCALATE
      priority: HIGH
```

## KYC/CDD 系统

### 客户尽职调查

- 开户时基础信息采集
- 身份证件验证（证件号码 + 有效期）
- 地址证明
- 职业/收入来源
- 交易目的声明

### 风险评级

- 客户风险等级: LOW / MEDIUM / HIGH / PROHIBITED
- 评级因子:
  - 国家风险评分
  - 产品风险评分
  - 客户类型（个人 / 企业 / PEP）
  - 交易模式评分
- 综合评分 = weighted_sum(factor_scores)

### 持续监控

- 定期复审: HIGH（每季度）、MEDIUM（每年）、LOW（每 2 年）
- 触发式复审: 交易模式异常变化时自动触发
- 复审结果更新 HBase 客户画像

## 制裁名单筛查

### 两阶段筛查

**Phase 1: Bloom Filter 快速过滤**
- 将制裁名单构建为 Bloom Filter，内存常驻
- 命中 → 进入 Phase 2
- 未命中 → 安全通过（O(1)）

**Phase 2: 精确模糊匹配**
- Levenshtein 距离 ≤ 2
- Jaro-Winkler 相似度 ≥ 85%
- 别名/曾用名匹配
- 命中 → 生成制裁告警，优先级 CRITICAL

### 名单源

- OFAC SDN（美国）
- EU Consolidated List（欧盟）
- UN Security Council（联合国）
- 自定义内部高风险名单

### 名单更新流程

- 新名单 → 重建 Bloom Filter → 原子切换 Redis
- 增量更新 → 合并到现有 Filter
- 更新审计日志

## 告警工作流 & 报送

### 告警生命周期

```
NEW → REVIEWING → ESCALATED → REPORTED → CLOSED
  └──→ CLOSED（误报排除）
```

### 工作流引擎

1. **告警接收 & 分配**: 从 Kafka/ClickHouse 消费新告警，按 severity + 规则类型自动分配审核员，SLA 计时开始
2. **审核界面（案件管理）**: 告警详情（交易信息 + 触发规则 + 客户画像），关联交易时间线，操作（确认可疑 / 排除误报 / 升级 / 添加备注）
3. **报告生成**:
   - CTR: 实时层自动生成，无需人工审核
   - SAR/STR: 人工确认后触发生成
   - 定期汇总: T+1 离线任务生成
4. **监管 API 对接**: 报告格式化（FATF XML/JSON 标准），提交队列 & 重试机制，提交状态跟踪 & 回执管理

### SLA 时效要求

| 告警级别 | 审核时限 | 报送时限 | 自动升级 |
|---------|---------|---------|---------|
| CRITICAL | 30 min | 2h | 超时自动升级主管 |
| HIGH | 2h | 6h | 超时自动升级主管 |
| MEDIUM | 8h | 24h | 超时提醒 |
| LOW | 24h | T+1 | 无 |

### 报告类型

| 报告类型 | 触发方式 | 生成链路 | 输出格式 |
|---------|---------|---------|---------|
| CTR | 实时自动 | Spark Streaming → ClickHouse | FATF XML |
| SAR/STR | 人工确认后 | API → 报告生成服务 | FATF XML |
| 定期汇总 | 定时 (T+1) | Spark Batch → Hive → ClickHouse | PDF + XML |
| 制裁命中 | 实时自动 | Streaming → 即时通知 | JSON + 邮件 |

## 项目结构

```
aml-streaming/
├── pom.xml                                    # Maven 父 POM
├── aml-common/                                # 公共模块
│   └── src/main/scala/com/aml/common/
│       ├── model/                             # 数据模型
│       ├── config/                            # 配置管理
│       ├── util/                              # 工具类
│       └── serialization/                     # Kafka SerDe
├── aml-streaming/                             # 实时处理模块（Spark Structured Streaming）
│   └── src/main/scala/com/aml/streaming/
│       ├── layer1/                            # Layer 1: 数据标准化
│       ├── layer2/                            # Layer 2: 特征加载 & 窗口计算
│       ├── layer3/                            # Layer 3: 规则引擎 & 制裁筛查
│       └── layer4/                            # Layer 4: 溯源 & 告警聚合
├── aml-batch/                                 # 批处理模块（Spark on Hive）
│   └── src/main/scala/com/aml/batch/
│       ├── feature/                           # 特征工程
│       ├── kyc/                               # KYC/CDD 批量评估
│       ├── report/                            # 报告生成
│       └── reconciliation/                    # 实时/离线对账
├── aml-service/                               # 业务服务模块（Spring Boot）
│   └── src/main/java/com/aml/service/
│       ├── alert/                             # 告警工作流
│       ├── case/                              # 案件管理
│       ├── rule/                              # 规则管理
│       ├── kyc/                               # KYC 管理
│       └── report/                            # 报告 & 监管对接
├── aml-web/                                   # 前端（管理后台）
├── docker/                                    # Docker 配置
├── docs/                                      # 文档
└── scripts/                                   # 运维脚本
```

## 防数据倾斜策略

- Layer 1: 按 `txn_id` 均匀分区
- Layer 2: 按 `customer_id` 分区，对高频客户使用加盐（salting）分散热点
- Layer 3: 无状态处理，可水平扩展
- Layer 4: 对溯源查询设置超时和深度限制，防止链路爆炸
- 旁路链路处理热点场景（高频客户、大额交易、制裁命中、批量入账）
