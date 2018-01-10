package com.examples.pig.udf

import org.apache.pig.data.Tuple
import org.apache.pig.data.TupleFactory
import spock.lang.Specification

class UpperTest extends Specification {
    private def tupleFactory

    def setup() {
        tupleFactory = TupleFactory.getInstance()
    }

    def "convert string to upper case"() {

        setup:

        Tuple inputTuple = tupleFactory.newTuple()
        inputTuple.append("hello")

        when:

        String result = new Upper().exec(inputTuple)

        then:

        result == 'HELLO'
    }
}