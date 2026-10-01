# MIT 6.824 / 6.5840: Distributed Systems (Spring 2026)

本仓库为 **MIT 6.824 / 6.5840 分布式系统** 的实验代码仓库与学习资料库。代码基于官方最新发布的 **Spring 2026** 版本（Go 1.22+ 模块化）。

## 快速导航

- 🧭 **学习指南与课程映射表**：请先阅读 [LEARNING_GUIDE.md](file:///Users/apple/Code/6.824/LEARNING_GUIDE.md)，查看全套 Lecture 视频、必读论文与 5 个 Lab 的详细对应关系与学习节奏。
- 🛡 **助教与协作规范**：在开始写代码前，请完整阅读 [AGENT.md](file:///Users/apple/Code/6.824/AGENT.md)，了解本仓库中 AI 助教的定位（代码审查、不变量评估、答辩陪练，禁止提供协议实现）。
- 📄 **实验讲义 PDF**：[docs/labs/](file:///Users/apple/Code/6.824/docs/labs/)
  - [Lab 1: MapReduce](file:///Users/apple/Code/6.824/docs/labs/Lab1-MapReduce.pdf)
  - [Lab 2: Key/Value Server](file:///Users/apple/Code/6.824/docs/labs/Lab2-KeyValue-Server.pdf)
  - [Lab 3: Raft Consensus](file:///Users/apple/Code/6.824/docs/labs/Lab3-Raft.pdf)
  - [Lab 4: Fault-Tolerant KV Raft](file:///Users/apple/Code/6.824/docs/labs/Lab4-KVRaft.pdf)
  - [Lab 5: Sharded KV Service](file:///Users/apple/Code/6.824/docs/labs/Lab5-ShardedKV.pdf)
- 📚 **经典论文库**：[docs/papers/](file:///Users/apple/Code/6.824/docs/papers/)（包含 MapReduce, GFS, Raft, Paxos, Spanner, ZooKeeper, FaRM, Chain Replication 等全部 15 篇必读 PDF）。
- 📝 **教授精炼讲义笔记**：[docs/notes/](file:///Users/apple/Code/6.824/docs/notes/)。
- 🔬 **Flaky Bug 多轮压测脚本**：[stress-test.sh](file:///Users/apple/Code/6.824/stress-test.sh)。

## 代码结构

```text
.
├── Makefile              # 顶层构建与检查
├── AGENT.md              # AI 助教角色约束与学术诚信规范
├── LEARNING_GUIDE.md     # Lecture 与 Lab 双向映射学习路线图
├── stress-test.sh        # 循环回归压测脚本（统计失败率）
├── docs/
│   ├── labs/             # 官方实验指导 PDF & HTML 离线文档
│   ├── papers/           # 必读分布式经典论文 PDF
│   └── notes/            # 官方 Lecture Notes 讲义精要
└── src/
    ├── mr/               # Lab 1: MapReduce
    ├── kvsrv1/           # Lab 2: Key/Value Server
    ├── raft1/            # Lab 3: Raft (3A, 3B, 3C, 3D)
    ├── kvraft1/          # Lab 4: KV Raft
    ├── shardkv1/         # Lab 5: Sharded KV
    ├── labrpc/           # 实验网络与 RPC 模拟库（支持丢包、乱序、延迟）
    └── tester1/          # 自动化评分与测试框架
```

## 快速上手

1. 确保已安装 Go 1.22+：
   ```bash
   brew install go
   ```
2. 运行 Lab 1 测试：
   ```bash
   cd src/main
   bash test-mr.sh
   ```
3. 运行多轮压测（例如对 Lab 3 Raft 3A 运行 20 次）：
   ```bash
   ./stress-test.sh raft1 3A 20
   ```
