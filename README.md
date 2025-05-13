<p align="center">
    <img src="figures/banner.png" title="matchbox" alt="matchbox" width="600">
</p>

A flexible read processor, capable of performing powerful transformations on your FASTA/FASTQ/SAM files.

You could use *matchbox* for:

- Quick, error-tolerant searches for primers and known sequences
- Validating the structure of your reads
- Quantifying and filtering out sequencing artefacts
- Demultiplexing even the most complex barcoding schemes

<a href="https://jakob-schuster.github.io/matchbox-docs/">Read the documentation</a>

## Installation

Clone the GitHub repo and build it using cargo. More accessible distribution coming soon!

```bash
git clone https://github.com/jakob-schuster/matchbox.git
cd matchbox
cargo build --release
```

## Todo

- [ ] add search term parameters
- [ ] add a terminal UI mode
