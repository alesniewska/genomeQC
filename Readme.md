
## Compilation

For compilation sbt is needed (tested on version 1.1.2). 
For running Apache Spark 2.4 compiled against scala 2.11 is required (tested on spark 2.4.3).

```bash
cd bigwig
sbt clean assembly
```

### Low coverage regions.

#### Running

```bash
spark-submit  --executor-cores 12 --executor-memory 60G --class genomeqc.CoverageMain genomeqc-assembly-0.1.jar "/data/*.bam" 10 /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
3. Path to output directory, where result files will be written to.

#### Description

This program takes one or more sorted BAM files and for every input file computes coverage, finds regions with coverage below specified value and saves those regions in BED files (one BED file for one input BAM). 

Afterwards it finds intersection of low coverage regions from all input files (regions, that have low coverage in every BAM file) and saves it to BED file. 


### Intersection with low mappability regions

#### Running

```bash
spark-submit  --executor-cores 12 --executor-memory 60G --class genomeqc.Mappability genomeqc-assembly-0.1.jar "/data/*.bam" 10 /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
3. Path to BigWig file with mappability track.
4. Mappability threshold. All regions with score less than this value will be considered as low mappability regions.
5. Path to output directory, where result files will be written to.

#### Description

This program finds intersection of low coverage regions from all BAM files and low mappability regions and saves it to BED file.

### Low covered genes

#### Running

```bash
spark-submit  --executor-cores 12 --executor-memory 60G --class genomeqc.LowCoveredGenesMain genomeqc-assembly-0.1.jar "/data/*.bam" 10 /data/homo_sapiens.gtf 0.6 /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
3. Path to GTF file.
4. Gene intersection ratio threshold. If low coverage regions make at least (100 * threshold) % of a gene, this gene will be included in the result.
5. Path to output directory, where result files will be written to.

#### Description

This program finds intersection of low coverage regions from all BAM files and combines it with gene data.

For every gene it calculates percentage of low covered nucleotides and saves to BED file genes with percentage above specified threshold.

### Coverage values for genes

#### Running

```bash
spark-submit  --executor-cores 12 --executor-memory 60G --class genomeqc.GeneSummaryMain genomeqc-assembly-0.1.jar "/data/*.bam" 10 /data/homo_sapiens.gtf /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage below this value with be considered as low coverage regions.
3. Path to GTF file.
4. Path to output directory, where result files will be written to.

#### Description

This program finds intersection of low coverage regions from all BAM files and combines it with gene data.

For every gene it calculates percentage of low covered nucleotides and saves it to BED file.

### High covered baits

#### Running

```bash
spark-submit --executor-cores 12 --executor-memory 60G --class genomeqc.HighCoveredBaitsMain genomeqc-assembly-0.1.jar "/data/*.bam" 60 /data/covered.bed /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage above this value with be considered as high coverage regions.
3. Path to BED file with baits.
4. Path to output directory, where result files will be written to.

#### Description

This program finds intersection of high coverage regions from all BAM files and intersects it once again with bait regions.

This intersection is saved to one BED file and to seconds BED file are saved only those baits that are entirely high covered.

### Coverage values for exons

#### Running

```bash
spark-submit --executor-cores 12 --executor-memory 60G --class genomeqc.ExonSummaryMain genomeqc-assembly-0.1.jar "/data/*.bam" 60 /data/homo_sapiens.gtf  /data/output
```

#### Parameters
1. Path to input BAM file or files. Quotes are necessary, so that the asterisk is interpreted by spark, not by shell.
2. Coverage threshold. All regions with coverage above this value with be considered as high coverage regions.
3. Path to GTF file.
4. Path to output directory, where result files will be written to.

#### Description

This program finds intersection of low coverage regions from all BAM files and combines it with gene data.

For every exon it calculates percentage of low covered nucleotides and saves to BED file genes with percentage above specified threshold.

## Sample data
1. Genome BAM file
https://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG003_NA24149_father/10XGenomics/NA24149_phased_possorted_bam.bam
2. Exome BAM file
ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/HG003_NA24149_father/OsloUniversityHospital_Exome/151002_7001448_0359_AC7F6GANXX_Sample_HG003-EEogPU_v02-KIT-Av5_TCTTCACA_L008.posiSrt.markDup.bam
2. GTF file
ftp://ftp.ensembl.org/pub/grch37/current/gtf/homo_sapiens/Homo_sapiens.GRCh37.87.chr.gtf.gz
3. Mappability regions file
http://hgdownload.cse.ucsc.edu/goldenpath/hg19/encodeDCC/wgEncodeMapability/wgEncodeCrgMapabilityAlign100mer.bigWig
4. BED file with baits
ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/analysis/OsloUniversityHospital_Exome_GATK_jointVC_11242015/wex_Agilent_SureSelect_v05_b37.baits.slop50.merged.list