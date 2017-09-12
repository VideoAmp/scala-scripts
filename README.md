# Scala Scripts

A smattering of useful [Scala scripts](http://ammonite.io/#ScalaScripts) for Ammonite and its kin, e.g. [Jupyter Scala](https://github.com/jupyter-scala/jupyter-scala). Contributions welcome.

## Usage

Each script file documents how to load and use it. For example, load [Postgresql.sc](postgresql/Postgresql.sc) with the [import $exec](http://ammonite.io/#import$exec) magic import

```scala
import $exec.postgresql.Postgresql
```

or the [interp.load.module](http://ammonite.io/#Multi-stageScripts) method

```scala
import ammonite.ops._
interp.load.module(pwd/'postgresql/"Postgresql.sc")
```
