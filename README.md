<p align="center">
    <img src="figures/banner.png" title="matchbox" alt="matchbox" width="500">
</p>

A flexible read processor, capable of performing powerful transformations on your FASTA/FASTQ/BAM files. 

You could use *matchbox* for:

- Quick, error-tolerant search for primers and known sequences
- Demultiplexing even the most complex barcoding schemes
- Investigating and filtering out sequencing artefacts

**<a href="https://jakob-schuster.github.io/matchbox-docs/">Read the documentation site</a>** for more examples, or **<a href="https://www.biorxiv.org/content/10.1101/2025.11.09.685711v1">check out the preprint</a>**.


# Installation

*matchbox* can be installed using cargo:

```bash
cargo install matchbox-cli
```

# Usage

Write your *matchbox* script in a `.mb` file, and give it to *matchbox* via `--script`.

```bash
matchbox -s my_script.mb my_reads.fq
```

- To allow for edit distance when searching for sequences, use `--error`
- To process data on multiple threads for improved speed, use `--threads`
- To handle paired reads, use `--paired-with`

**[For examples and a full scripting language reference, read the documentation!](https://jakob-schuster.github.io/matchbox-docs/)**

# Citation

If you use *matchbox*, cite the preprint at [DOI:10.1101/2025.11.09.685711 ](https://www.biorxiv.org/content/10.1101/2025.11.09.685711v1)