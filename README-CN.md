# dora-test-utils

> **DORA 版本**：基于已发布的 **dora 1.0.1**（`dora-node-api = { version = "1.0.1", features = ["arrow-v59"] }`，arrow 59）。`arrow-v59` feature 是必需的：它提供 `DoraArray` 的访问方法以及 Arrow 59 数组的 `IntoArrow` 实现。请安装匹配的 `dora-cli`（`cargo install dora-cli --locked --version 1.0.1`），避免 CLI 与库 API 不一致。
>
> 中文版文档。English version: [README.md](README.md)

为 [DORA](https://dora-rs.ai/) 数据流框架提供测试支持的 Rust 工具库。DORA 开发者可以用它像测试普通 Rust 代码一样测试自己的节点：**单节点测试不用起 daemon，整条流水线测试不改被测代码，回归测试录一次基线、以后自动比对**。

## 三层测试怎么做

```
┌──────────────────────────────────────────────────┐
│  Layer 1: NodeHarness — 单元测试                  │
│  不起 daemon，在 #[test] 里驱动单个节点            │
├──────────────────────────────────────────────────┤
│  Layer 2: TestSource / TestSink — 集成测试        │
│  写进真实 YAML dataflow，端到端验证               │
├──────────────────────────────────────────────────┤
│  Layer 3: Record / Replay — 回归测试              │
│  录制一次真实运行 → 以后每次重放比对               │
└──────────────────────────────────────────────────┘
```

### Layer 1：单元测试（NodeHarness）

**机制**：NodeHarness 在测试进程里创建真实的 DORA 节点，输入用提前准备好的事件喂给它——不需要 daemon、不需要 YAML、不需要环境变量。测试代码注入输入、逐步驱动节点的事件循环、把节点产生的输出从内存通道取出来断言。

推荐把节点业务逻辑抽成库函数：**逻辑只写一次**，测试直接断言这个函数（便宜、可穷举边界），再用 harness 跑一遍事件循环验证接线（数据解析、触发、输出）。两者互补——一个测"算得对不对"，一个测"接得对不对"。

```rust
let mut harness = NodeHarness::new()?;
harness.send_data("image", serde_json::json!([1, 2, 3]));  // 注入输入
while let Some(event) = harness.tick() { /* 节点逻辑处理事件 */ }
let outputs = harness.recv_output("result");               // 捕获输出
assert!(outputs.is_some());
```

### Layer 2：集成测试（TestSource / TestSink）

**机制**：不改被测节点代码，只在 YAML 里加两个现成节点——入口一个送料机（`test-source`，从 JSON 文件读数据逐个发出），出口一个质检员（`test-sink`，把收到的数据和预期文件比对，写 `match: true/false` 结果）。整条流水线用 `dora run` 真实跑一遍，CI 检查结果文件的 `match`。

现成的二进制节点：

| 二进制 | 作用 |
|--------|------|
| `test-source` | 从 JSON 文件读数据发到 DORA 输出（支持多输出） |
| `test-sink` | 接收数据，与预期文件比对，写匹配结果 |
| `echo-node` | 透传，验证链路连通性 |
| `classifier-node` | 按阈值分流到两个输出 |
| `distance-guard` | 示例节点：距离小于安全阈值（`--safety-distance`，默认 0.5m）发急停 |
| `trajectory-node` | 示例节点：7 关节轨迹线性插值（`--steps` 控制分辨率）—— Layer 3 回归 demo 的被测节点 |

比对支持两种模式：**语义比对**（默认，转 Arrow 比数值，容忍 Int32 vs Int64 这类类型差异）和**严格比对**（JSON 逐值相等）。

### Layer 3：回归测试（RecordSession / ReplaySession）

**机制**：给流水线拍一张"出厂合格照"——第一次跑时把每个质检员收到的数据录成基线（含接线图路径、dora 版本、超时等元数据）；以后代码或配置变了，重放一遍和基线自动比对，`is_clean()` 告诉你有没有回归，`DiffReport` 报出哪个 sink、哪个字段、从什么变成了什么。

真实流水线总有非确定性噪音（tick 计数、时间戳、调试日志），用两个过滤原语声明跳过：

```rust
ReplaySession::load("baseline.json")?
    .replay_sink("test-sink", "out.json")
    .ignore_paths(&["count", "timestamp"])  // 跳过这些字段（支持后代如 data[0]）
    .ignore_sink("debug-log")               // 跳过整个 sink
    .run()?
    .assert_no_regression();                // 有回归就 panic，CI 红
```

比对分两层：JSON 结构 diff（快速定位）+ Arrow 语义比对（容忍类型差异）。DiffReport 区分 `Match` / `Mismatch` / `Missing` / `Extra` 四种状态，字段级差异带路径和前后值。

## 支持的数据类型

- **整数**：Int8/16/32/64、UInt8/16/32/64（带溢出检查，超过 2^53 也精确比较）
- **浮点**：Float32/64
- **字符串**：String、LargeString
- **其他**：Boolean、Null、数组、对象（Struct）

注入和比对统一走 Arrow 线格式；JSON 输入通过 `data_type` 字段提示目标类型。

## 可运行的 Demo

全部围绕 **Realman GEN72 七轴机械臂** 一个主题，三层各自独立可跑：

| Demo | 入口 | 内容 |
|------|------|------|
| 单元测试 | `cargo run --example harness_demo` | GEN72 关节限位监测：Part A 直测逻辑（含 J4/J6 不对称限位的边界用例）+ Part B 经 harness 跑事件循环 + **Part C 故意造一个 bug 并展示测试如何抓住它** |
| 集成测试 | `bash scripts/demo-integration.sh` | 四条真实流水线：七轴配置回传（echo）、关节位置 + 速度双路（multi-echo）、末端防撞急停（distance-guard）、**配错的 distance-guard（安全距离设太低，被质检员抓出 match:false）** |
| 回归测试 | `cargo run --example demo_replay` | 轨迹插值节点录制基线（140 个轨迹值）→ 插值分辨率 10→5 步（真实运动控制回归）→ 检测 67 处差异 |
| **一键总览** | `bash scripts/demo-final.sh` | 三层连放 + 完整测试套件；自动安装匹配的 `dora-cli` |

另外 `demo/rust-dataflow.yml` 保留了工具对 DORA 官方 rust-dataflow example 零修改用法的参考示例。

## 快速上手

**前置条件**：Rust 工具链；跑集成/回归测试需要 dora CLI（demo-final.sh 会自动 clone 并构建）。

```bash
# 构建全部二进制
cargo build --bin test-source --bin test-sink --bin echo-node \
    --bin classifier-node --bin distance-guard --bin trajectory-node

# 跑测试（共 122 个：91 单元 + 5 e2e + 4 record + 13 replay + 6 integration + 3 smoke）
cargo test --lib                          # 单元测试
cargo test --test e2e_replay -- --test-threads=1   # 回归 e2e（需要 dora CLI）
bash scripts/demo-final.sh                # 一键：三层 demo + 全部测试
```

## 项目结构

```
src/               # 库：harness（单元测试驱动）、source/sink（数据注入与比对）、
                   #     record（录制/回放/差异报告）、mock（无 daemon mock）、
                   #     bin/（test-source、test-sink、echo、classifier、
                   #           distance-guard、trajectory-node 等二进制）
tests/             # fixtures（静态 YAML + 数据文件，可直接 dora run）+
                   # 各层测试套件
examples/          # harness_demo.rs（Layer 1）、demo_replay.rs（Layer 3）
demo/              # Layer 3 的接线图与数据文件
scripts/           # demo-integration.sh（Layer 2）、demo-final.sh（总编排）
docs/              # 设计文档、进度记录
```

## API 稳定性

| API | 状态 |
|-----|------|
| `NodeHarness`、`TestSource`/`TestSink`、`MockEventStream`/`MockOutputSender`、`IntoInputData` | **Stable** |
| `RecordSession`/`Recording`、`ReplaySession`/`ReplayResult`、`DiffReport` 系列 | **Experimental** |

## CI

`.github/workflows/ci.yml` 五个 job：`check`、`test`（lib + e2e + smoke）、`clippy`、`fmt`、`integration-test`（clone dora 到 pin 住的 commit、构建 CLI、串行跑集成 + record/replay e2e，30 分钟上限）。

## 进度

Week 1-11 全部完成（API 设计、NodeHarness、TestSource/TestSink、CI、Record/Replay、DORA 升级）；Week 12-13 完成 demo 打磨（三层 GEN72 主题化 + 两轮 code review 修复）。详见 [`docs/PROGRESS.md`](docs/PROGRESS.md)。

## 许可

本项目为 GSoC 2026 项目，最终将合入 [dora-rs/dora](https://github.com/dora-rs/dora) 主仓库。
