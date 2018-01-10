package com.examples.pig;

import org.apache.pig.ExecType;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;

import java.io.IOException;

// Used to speed up pig tests with multiple tests in the same test class, as per
// http://www.millennialmedia.com/mobile-insights/blog/a-solution-to-lengthy-pigunit-tests
class PigUnitUtil {
  static PigTest createPigTest(String scriptFile, String[] inputs) throws IOException {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Cluster pigCluster = new Cluster(pigServer.getPigContext());
    return new PigTest(scriptFile, inputs, pigServer, pigCluster);
  }
}