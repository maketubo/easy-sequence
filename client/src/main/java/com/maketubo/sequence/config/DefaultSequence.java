package com.maketubo.sequence.config;

import com.maketubo.sequence.annotation.SeqNameField;
import com.maketubo.sequence.annotation.SeqNextValField;
import com.maketubo.sequence.annotation.SequenceEntity;

/**
 * @author maketubo
 * @version 1.0
 * @ClassName DefaultSequence
 * @description
 * @date 2020/8/17 22:20
 * @since JDK 1.8
 */
@SequenceEntity(tableName = "EASYSEQUENCE")
public class DefaultSequence {

    @SeqNameField
    private String sequenceName;
    @SeqNextValField
    private Long nextSeqNumber;


    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public Long getNextSeqNumber() {
        return nextSeqNumber;
    }

    public void setNextSeqNumber(Long nextSeqNumber) {
        this.nextSeqNumber = nextSeqNumber;
    }
}
