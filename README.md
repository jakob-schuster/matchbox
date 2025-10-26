<p align="center">
    <img src="figures/banner.png" title="matchbox" alt="matchbox" width="600">
</p>

A flexible read processor, capable of performing powerful transformations on your FASTA/FASTQ/SAM files.

You could use *matchbox* for:

- Quick, error-tolerant searches for primers and known sequences
- Validating the structure of your reads
- Quantifying and filtering out sequencing artefacts
- Demultiplexing even the most complex barcoding schemes

**<a href="https://jakob-schuster.github.io/matchbox-docs/">Read the documentation</a>**


# Installation

*matchbox* can be installed using cargo:

```bash
cargo install matchbox-cli
```

# Usage

Write your matchbox script in a `.mb` file, and give it to *matchbox* via `--script`.

```bash
matchbox -s my_script.mb my_reads.fq
```

- To allow for edit distance when searching for sequences, use `--error`
- To process data on multiple threads for improved speed, use `--threads`
- To handle paired reads, use `--paired-with`

**[For examples and a full scripting language reference, read the documentation!](https://jakob-schuster.github.io/matchbox-docs/)**
