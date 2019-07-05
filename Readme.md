## Compilation

For compilation sbt is needed (tested on version 1.1.2)

```bash
sbt clean assembly
```
## Running

Spark 2.4 compiled against scala 2.11 is required to run this program (tested on spark 2.4.3).

```bash
spark-submit  --executor-cores 4 --driver-memory 12G --class CoverageMain bigwig-generator-assembly-0.1.jar "/data/*.bam" /data/homo_sapiens.gtf 3 0.5 /data/output /data/hg18.chrom.sizes /data/mappability.bigWig 0.5
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Path to GTF file.
3. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
4. Gene intersection ratio threshold. If low coverage regions make at least (100 * threshold) % of a gene, this gene will be included in the result.
5. Path to output directory, where result files will be written to.
6. Path to text file with chromosome sizes, which is needed for creating BigWig files. It can be donwloaded for example with [fetchChromSizes](http://hgdownload.cse.ucsc.edu/admin/exe/linux.x86_64/fetchChromSizes) utiility.
7. Path to BigWig file with mappability track.
8. Mappability threshold. All regions with score above or equal to this value will be joined with other results.

## Description

This program takes one or more sorted BAM files and for every input file computes coverage, finds genome regions with coverage below specified value and saves those regions in BigWig files (one BW file for one input BAM). 

Afterwards it finds intersection of low coverage regions from all input files (regions of genome, that have low coverage in every BAM file) and saves it to BigWig file. 

Intersection of low covered regions is further joined with genes from GTF file. For every gene it computes length of intersection with low coverage regions and divides it by length of the gene. If result is greater or equal to specified threshold value low covered regions of this gene are included in two BigWig files (one for genes with positive strand and one for negative). Genes that consist entirely of low covered regions are written to two additional BW files (one per strand). 

Intersection of low coverered regions from all inputs is then joined with regions, that have mappability score greater or equal to specified threshold and result is written to one more BigWig file.
