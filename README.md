# Septima.js

[![Build Status](https://travis-ci.org/marat-gainullin/septima.svg?branch=master)](https://travis-ci.org/marat-gainullin/septima)

The Septima.js is a platform for rapid development of enterprise-level JavaScript applications.

Septima.js is a server side JavaScript platform designed on top of JavaEE 7+ technologies and Nashorn JavaScript engine.

Septima.js is a client side development platform as well including HTML 5 browser.

Septima.js comprises both asynchronous IO model and reasonable use of multithreading with only few intensively used threads.

Septima.js provides a developer with libraries needed to develop complete and ready for market applications.

In general, two programming languages are used while developing Platypus.js applications:
* JavaScript (ECMAScript 5 and ECMAScript 6 partial) for application logic implementation both on server side and client side.
* Sql for accessing relational databases.

Applications creation, editing, deployment, debugging and maintenance all may be performed using the NetBeans IDE with Platypus.js plugins.

The Platypus.js plugins for NetBeans IDE include the following development tools:

* Database structure visual editor.
* Sql columns visual editor.
* JavaScript code editor.
* ORM configuration visual editor.
* User interface forms visual editor.

## Features
The platform offers a set of features, making development process extremely productive:
* AMD JavaScript modules loader on server side.
* Server side JavaScript modules within IoC paradigm with LPC (Local procedure calls) asynchronous interactions.
* Parallel execution scheme of JavaScript code using standard Java thread pools.
* Sql columns with named parameters and Sql clauses reuse.
* ORM (Object-Relation Mapping) for JavaScript with automatic inter-entities references resolving. 
* The UI widgets which directly interact with the data model, allowing implementation of a CRUD (Create-Read-Update-Delete) user interface with visual configuration and without or just a little coding.
* Built-in security support, including users authentication and authorization, constraints of access to application resources.

## Build
The following commands will clone git repository and build Septima.js from source
```
git clone https://github.com/marat-gainullin/septima.git
cd septima
gradlew build
```

## Test
The following command will run all tests for Platypus.js
```
gradlew test
```

## Demos
There are some sample Septima.js applications, showing how to build and run Septima.js applications.
They are in the following repositories:
https://github.com/altsoft/UiDemo
https://github.com/altsoft/pethotel
https://github.com/altsoft/WebSocketDemo