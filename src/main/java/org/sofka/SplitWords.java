package org.sofka;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SplitWords extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> line) {

        PCollection<String> lines = line.apply(
                ParDo.of(new SplitWordsFn()));
        return lines;
    }
}
