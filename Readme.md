
## Compilation

For compilation sbt is needed (tested on version 1.1.2)

```bash
cd bigwig
sbt clean assembly
```
# Computing low covered genes

## Running

Spark 2.4 compiled against scala 2.11 is required to run this program (tested on spark 2.4.3).

```bash
spark-submit  --executor-cores 4 --driver-memory 9G --class CoverageMain bigwig-generator-assembly-0.1.jar "/data/*.bam" /data/homo_sapiens.gtf 3 0.5 /data/output /data/mappability.bigWig 0.5
```

## Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Path to GTF file.
3. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
4. Gene intersection ratio threshold. If low coverage regions make at least (100 * threshold) % of a gene, this gene will be included in the result.
5. Path to output directory, where result files will be written to.
6. Path to BigWig file with mappability track.
7. Mappability threshold. All regions with score above or equal to this value will be joined with other results.

## Description

This program takes one or more sorted BAM files and for every input file computes coverage, finds regions with coverage below specified value and saves those regions in BED files (one BED file for one input BAM). 

Afterwards it finds intersection of low coverage regions from all input files (regions, that have low coverage in every BAM file) and saves it to BED file. 

Intersection of low covered regions is further joined with genes from GTF file. For every gene it computes length of intersection with low coverage regions and divides it by length of the gene. If result is greater or equal to specified threshold value low covered regions of this gene are included in two BED files (one for genes with positive strand and one for negative). Genes that consist entirely of low covered regions are written to two additional BED files (one per strand). 

Intersection of low coverered regions from all inputs is then joined with regions, that have mappability score greater or equal to specified threshold and result is written to one more BED file.

## Sample data
1. BAM files
[https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG002_NA24385_son/10XGenomics/NA24385_phased_possorted_bam.bam](https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG002_NA24385_son/10XGenomics/NA24385_phased_possorted_bam.bam)
[https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG003_NA24149_father/10XGenomics/NA24149_phased_possorted_bam.bam](https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG003_NA24149_father/10XGenomics/NA24149_phased_possorted_bam.bam)
[https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG004_NA24143_mother/10XGenomics/NA24143_phased_possorted_bam.bam](https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG004_NA24143_mother/10XGenomics/NA24143_phased_possorted_bam.bam)
2. GTF file
ftp://ftp.ensembl.org/pub/grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.chr.gtf.gz
3. Mappability regions file
http://hgdownload.cse.ucsc.edu/goldenpath/hg19/encodeDCC/wgEncodeMapability/wgEncodeCrgMapabilityAlign100mer.bigWig

# Exome variant

## Running

```bash
spark-submit --executor-cores 4 --driver-memory 9G --class ExomeVariantMain bigwig-generator-assembly-0.1.jar "/data/*.bam" /data/covered.bed /data/homo_sapiens.gtf 10 /data/output
```

## Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Path to exomie kit BED file with baits.
3. Path to GTF file.
4. Coverage threshold. All regions with coverage above this value with be considered as high coverage regions.
5. Path to output directory, where result files will be written to.

## Description

This program takes one or more sorted BAM files and for every input file computes coverage, finds regions with coverage above specified value and saves those regions in BED files (one BED file for one input BAM). 

Afterwards it finds intersection of high coverage regions from all input files (regions, that have low coverage in every BAM file) and saves it to BED file. 

Intersection of high covered regions is further joined with intervals from exom kit file and to one BED file high covered regions of exome kit are save and to second baits consisting entirely of high coverage regions.

Similarly intersection of high coverage regions with genes and exons are saved to BED files (four files for genes and four for exons as separate files for two strand are created).

Finally text file containing list of all genes with computed ratios of exon regions covered by high coverage intervals is saved.

## Sample data

1. BAM files
2. GTF file
ftp://ftp.ensembl.org/pub/grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.chr.gtf.gz
3. Exome kit file
S04380110 from https://earray.chem.agilent.com/suredesign/search.htm (available after signing in)