package com.examples.pig

import org.apache.pig.pigunit.PigTest
import spock.lang.Specification

import static PigUnitUtil.createPigTest

class WordCountTest extends Specification {
    private static final String TEST_PATH = 'src/integration-test/resources/market3Billing/'
    private static final String PIG_SCRIPT_PATH = 'src/main/resources/'

    PigTest test

    private def pigParams(inputDataFilename) {
        [
                "REPORT_HOME=../UDFs/build",
                "INPUT=${TEST_PATH}input/${inputDataFilename}"
        ]
    }

    def "word count upper case"() {
        when:

        String[] params = pigParams(inputDataFilename)

        test = createPigTest("${PIG_SCRIPT_PATH}wordcountUpper.pig", params)

        then:

        String[] output = [
                '(A,4)',
                '(IS,3)',
                '(FACT,1)',
                '(POST,1)',
                '(THIS,2)',
                '(KNOWN,1)',
                '(HADOOP,2)',
                '(STORES,1)',
                '(BIGDATA,1)',
                '(RECORDS,1)',
                '(DATABASE,1)',
                '(TECHNOLOGY,1)',
        ] as String[]

        expect:

        test.assertOutput("wordcount", output)

        where:

        inputDataFilename = 'example.txt'
    }
}