# Septima Java & Sql

The Septima is a data driven framework for building Java applications.

Septima provides a developer with libraries needed to develop complete and ready for market applications.

In general, two programming languages are used while developing Septima applications:
* Java 9 for application logic implementation on server side.
* Sql for constructing of Sql entities.

## Features
* Septima is fully asynchronous.
* Servlet 3.1 is supported while request reading, processing and response writing.
* Java 9 `java.util.concurrent.Flow` execution scopes for session driven logic and for stateless logic.
* Role based security supported out of the box.
* True Sql queries reuse.
* Sql entities ORM-like data model.
* Automatic mapping without any configuration.
* No annotations on domain classes.
* No reflection at runtime as well as at design time.
* Automatic and secure REST endpoints for publishing of Sql entities' data.

## Sql queries reuse
Imagine, you have a file `untyped-pets.sql` with sql query like this:
```
Select * from pets p where p.owner_id = :owner and p.type is null
```
And now you can write another sql query like this:
```
Select * from owners o inner join #./untyped-pets p
```
Thus, you can never repeat yourself while writing sql queries.
Absolute paths from an application resources root and relative paths like `../../general/a.sql` or `./a.sql` are supported following a hash sign.

## Sql entities instead of table entities & ORM
`septima-model` and `septima-generator` sub-projects offer a data model similar to an ORM, but with some key differences:
* Domain entities are constructed upon Sql queries, instead of database tables and they are referred to as `Sql Entities`.
* Database tables are not projected to your program classes.
* If you write two queries like these: `Select p.name, p.id from pets p` and `Select * from pets p` it will lead to creation of two distinct entities in your program.
* All mapping is done via pure functional mappers and no reflection of any kind is used.
* Navigation properties are used as well as theirs source primitive properties.
So, you can assign an instance of `Owner` to pet's `owner` property, or you can assign an `id` of owner to pet's `owner_id` property. Results will be the same.

## Data model
Generated data model is statically typed and type safe.
Data model adds navigation properties of scalar and collection types.
'Scalar' navigation property is dereferenced key and 'Collection' navigation property is a collection of instances referencing this instance from another entity.
The data model generator in many cases can resolve inter entities references automatically without a configuration/annotations, etc.

Such data model leads to clear abstraction of application logic from relational data store, because database structure doesn't govern your program structure *any more*.

## Build
The following commands will clone git repository and build Septima from source
```
git clone https://github.com/marat-gainullin/septima.git
cd septima
gradlew build
```

## Test
The following command will run all tests for Septima
```
gradlew test
```
