Dremio Cluster Cloner
===

Overview
--

A library and command line utility for cloning one Dremio cluster into another.

Usage
--

Install:

```
npm i dremio-cluster-cloner
```

For help on the utility just type in:

```
dremclone
```

Extract the state of a cluster and save into a file:

```
dremclone save --host=localhost --port=9048 --user=user --password password
```

By default this will create a ```/dremio_cluster_state.json``` file in the current working directory.

Note, that no sensitive details are saved. So before loading the state into a new cluster, two files are required:

A user credentials file which looks like this:

```json
[{
  "userName": "user1",
  "password": "password1"
}, {
  "userName": "user2",
  "password": "password2"
}]
```

and a source credentials file which looks like this:

```json
{
  "sourceName": {
    "sensitiveProperty1": "sensitiveValue1",
    "sensitiveProperty2": "sensitiveValue2"
  }
}
```

So now, to load the state into your new cluster do:

```
dremclone load --host=localhost --port=9048 --user=user --userCredFile=user_creds.json --createFirstUser=true --sourceCredFile=source_creds.json
```
