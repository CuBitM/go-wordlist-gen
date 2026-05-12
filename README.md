# go-wordlist-gen
⚠️ Disclaimer

This tool is for educational and ethical security testing purposes only. The author is not responsible for any misuse.
🚀 Key Features

* **Blazing Fast:** Leverages Go's concurrency with worker pools and optimized memory management (`sync.Pool`).
* **Smart Mutation:** Intelligent generation based on language data (months, days, keyboard walks).
* **Bloom Filter:** Built-in deduplication using a Bloom Filter to keep memory usage low (approx. 7MB for 5M items).
* **Multi-Language Support:** Localized mutations for `cs`, `en`, `de`, `ru`, and `es`.
* **Rule Engine:** Support for custom mutation rules (compatible with standard rule formats).
* **Zero-Allocation Pipeline:** Batch processing for efficient disk I/O.

## 🛠 Installation

Make sure you have Go installed:

```bash
go build -o wordlist-gen main.go
