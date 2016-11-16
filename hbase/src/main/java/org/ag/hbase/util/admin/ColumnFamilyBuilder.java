package org.ag.hbase.util.admin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;

/**
 * Created by ahmed.gater on 16/11/2016.
 */
public class ColumnFamilyBuilder {

    private Compression.Algorithm compressionAlgo = Compression.Algorithm.GZ;
    private BloomType bloom = BloomType.ROWCOL;
    private int minVersion = 1;
    private int maxVersion = 5;

    public ColumnFamilyBuilder() {

    }

    public ColumnFamilyBuilder versions(int min, int max) {
        this.minVersion = min;
        this.maxVersion = max;
        return this;
    }

    public ColumnFamilyBuilder compression(Compression.Algorithm ca) {
        this.compressionAlgo = ca;
        return this;
    }

    public ColumnFamilyBuilder bloomFilter(BloomType bt) {
        this.bloom = bt;
        return this;
    }

    public HColumnDescriptor build(String cfNale) {
        return new HColumnDescriptor(cfNale)
                .setVersions(this.minVersion,this.maxVersion)
                .setBloomFilterType(this.bloom)
                .setCompactionCompressionType(this.compressionAlgo) ;
    }

}




