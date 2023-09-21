# Standard Operations

## Read, Write, and Delete

The core [building blocks](./building-blocks.md) of Artigraph can be interacted with
using the following functions.

| Object-based functions                 | Query-based functions            |
| -------------------------------------- | -------------------------------- |
| [delete_many()][artigraph.delete_many] | [delete()][artigraph.delete]     |
| [delete_one()][artigraph.delete_one]   | [exists()][artigraph.exists]     |
| [write_many()][artigraph.write_many]   | [read()][artigraph.read]         |
| [write_one()][artigraph.write_one]     | [read_one()][artigraph.read_one] |

### Object-based Functions

The object-based functions take [GraphObject][artigraph.GraphObject]s as arguments and
either write them to the database or delete them from the database. Usage tends to look
like this:

```python
import artigraph as ag

node = ag.Node()
ag.write_one(node)
ag.delete_one(node)
```

### Query-based Functions

The query-based functions take [Filter][artigraph.Filter] objects
([learn more](./filtering.md)) as arguments and either read or delete objects from the
database. Usage tends to look like this:

```python
import artigraph as ag

node = ag.Node()
ag.write_one(node)

node = ag.read_one(ag.NodeFilter(id=node.graph_id))
ag.delete(ag.NodeFilter(id=node.graph_id))
```

## Sessions

By default, Artigraph will create a new database session for each function call you
make. This is fine for most use cases, but if you're making a lot of calls to the
database, it can be more efficient to use a single session for all of your calls. You
can do this by using the [current_session()][artigraph.current_session] context manager:

```python
import artigraph as ag

with ag.current_session() as session:
    node = ag.Node()
    ag.write_one(node)
    node = ag.read_one(ag.NodeFilter(id=node.graph_id))
    ag.delete_one(node)
```

## Async Usage

Artigraph is designed for both synchronous and asynchronous usage. To allow for this,
Artigraph uses a bit of magic to figure out if it should run synchronously or
asynchronously depending on the context. In short, if there's a running event loop,
Artigraph will run asynchronously, and if there's not, it will do so synchronously.

!!! note

    Jupyter Notebooks and IPython shells have a running event loop by default. If you're
    using Artigraph in a Jupyter Notebook or IPython shell, you'll need to call Artigraph
    functions asynchronously.

For example, [write_one()][artigraph.write_many] can be used synchronously like this:

```python
import artigraph as ag

node = ag.Node()
ag.write_many(node)
```

While asynchronous use looks like this:

```python
import asyncio
import artigraph as ag


async def main():
    node = ag.Node()
    await ag.write_one(node)


asyncio.run(main())
```

Context managers also work in both synchronous and asynchronous contexts:

```python
with ag.current_session() as session:
    ...
```

And the async counter-part:

```python
import asyncio


async def main():
    async with ag.current_session() as session:
        ...


asyncio.run(main())
```

### Explicit Sync or Async Usage

Sometimes you may run into situations where the implicit sync or async behavior
described above doesn't work for you. For example, you need to call one of these
functions synchronously even though there's a running event loop. In these cases, you
can force Artigraph to use the sync or async version of a function by accessing the `.a`
or `.s` attributes of the function you're calling.

For example, if you needed to use the synchronous version of
[write_one][artigraph.write_one] in the presebnce of a running event loop, you can do so
like this:

```python
import asyncio
import artigraph as ag


async def main():
    node = ag.Node()
    ag.write_one.s(node)


asyncio.run(main())
```

Being able to be explicit about whether you're using a sync or async version of a
function can also be useful when working with type checkers since the return type of the
dual-use function (without the `.a` or `.s`) will be the union `Awaitable[T] | T` where
`T` is the return type of the function.
