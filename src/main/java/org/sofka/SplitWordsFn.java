package org.sofka;

import org.apache.beam.sdk.transforms.DoFn;

import java.util.Objects;

public class SplitWordsFn extends DoFn<String, String> {

    public static final String SPLIT_PATTERN = ":";

    @ProcessElement
    public void processElement(ProcessContext c) {
        for (String word: Objects.requireNonNull(c.element()).split(SPLIT_PATTERN)) {
            if (!word.isEmpty()) {
                c.output(word);
            }
        }
    }
}