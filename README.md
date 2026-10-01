# AI-QoSR: QoS-Aware Route Recommendation in SDN

Companion repository for the paper *"QoS-Aware Route Recommendation in SDN: Stability-Aware Model Selection from SLA-Labeled Telemetry."* It contains the SDN testbed implementation (Mininet topology and Ryu controller), the traffic-generation scripts, the collected per-path datasets, and the notebook used to produce all tables and figures in the paper.

## Repository Contents

| File | Description |
|---|---|
| `topoloji_kurucu_mesh.py` | Builds the 4-path Mininet/Open vSwitch mesh topology (edge switches, core switches, hosts, link profiles, QoS queues). |
| `data_collection_controller.py` | Ryu OpenFlow 1.3 controller. Installs static, single-active-path flow rules and the DSCP-to-queue (VIP/MEDIUM/LOW) mapping. |
| `advanced_data_generator.py` | Runs on host h1. Generates background traffic for the MEDIUM/LOW classes and probes all three QoS classes to produce labeled telemetry. |
| `dataset_path_a.csv` ... `dataset_path_d.csv` | Collected telemetry for paths A–D (see Table 2 and Table 4 of the paper for the path profiles and schema). |
| `AI_QoSR.ipynb` | Data preprocessing, model-selection framework, and evaluation. Produces Tables 3–6 and Figs. 1–4. |
| `LICENSE` | Repository license. |

## Requirements

- A Linux environment with root/sudo access (the testbed was built and run on a Linux VM; e.g., via UTM on macOS)
- [Mininet](http://mininet.org/) with Open vSwitch
- [Ryu](https://ryu-sdn.org/) (OpenFlow 1.3)
- Classic `iperf` (v2.x — **not** `iperf3`; see note in Step 3)
- Python 3 with `pandas`, `numpy`, `scikit-learn`, `xgboost`, `matplotlib`, `seaborn`, `joblib` for the notebook

> *Exact tool versions used for the paper's results to be added here — see "To confirm" note below.*

## 1. Start the SDN Controller

```bash
ryu-manager data_collection_controller.py
```

By default this activates whichever path is currently set by `ACTIVE_PATH_PORT` at the top of `data_collection_controller.py` (`2` = Path A, `3` = Path B, `4` = Path C, `5` = Path D). To collect data for a different path, edit that value and restart `ryu-manager`.

## 2. Build the Topology

In a separate terminal:

```bash
sudo python3 topoloji_kurucu_mesh.py
```

This connects to the controller at `127.0.0.1:6633`, builds the 4-path mesh, configures the per-path OVS QoS queues, and drops into the Mininet CLI.

## 3. Collect Telemetry for a Path

Start a classic `iperf` UDP server on h2 (required once per Mininet session):

```
mininet> h2 iperf -s -u -i 1 &
```

Run the generator on h1, labeling the output with the currently active path:

```
mininet> h1 python3 advanced_data_generator.py --duration 180 --output dataset_path_a.csv --target-ip 10.0.0.2 --path-label PathA_20ms
```

Repeat Steps 1–3 for each of the four paths (`PathB_2ms`, `PathC_10ms`, `PathD_50ms`), changing `ACTIVE_PATH_PORT` and restarting the controller between runs. The four resulting CSVs are included in this repository.

## 4. Reproduce the Analysis

Open `AI_QoSR.ipynb` (written for Google Colab). Place the four CSVs in the Drive folder referenced by the `path` variable in the first data-loading cell (or edit that variable to point to a local directory if running under Jupyter instead of Colab), then run all cells. This reproduces the dataset statistics, the six-candidate model-selection framework (Table 5), the selected model's evaluation (Table 6, Figs. 1–4), and the exported model artifacts (`.pkl` files).

## Citation

If you use this code or dataset, please cite:

> [Author names, "QoS-Aware Route Recommendation in SDN: Stability-Aware Model Selection from SLA-Labeled Telemetry," *IEEE Transactions on Machine Learning in Communications and Networking*, under review.]

Full citation details will be added upon publication.

## License

See [LICENSE](./LICENSE).
