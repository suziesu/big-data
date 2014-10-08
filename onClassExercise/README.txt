Compile
-------

    javac -cp hadoop-1.0.3/hadoop-core-1.0.3.jar WordCount.java

Create jar file
---------------

    jar cvf WordCount.jar *.class

    # or

    zip -r WordCount.jar *.class

Run
---

    cd hadoop-1.0.3/
    bin/hadoop jar ../WordCount.jar WordCount ../input ../output