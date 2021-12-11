package com.edge.r2dbc;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class SeqNum {
    private int senderSeqNum;
    private int targetSeqNum;
}
