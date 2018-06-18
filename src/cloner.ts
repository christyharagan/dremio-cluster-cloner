import { API, ExistingUser, createAPI, createFirstUser, schema, ClusterConfiguration, getDatasetsFromSQL } from 'dremio-node-api'
import { Writable } from 'stream';
import { EOL } from 'os';

async function flatMap<T, U>(array: T[], mapFunc: (x: T) => Promise<U[] | U>): Promise<U[]> {
  let a = <U[]>[]

  for (let i = 0; i < array.length; i++) {
    const mf = await mapFunc(array[i])
    if (Array.isArray(mf)) {
      a = a.concat(mf)
    } else {
      a.push(mf)
    }
  }
  return a
}

async function getEntities(api: API) {
  function handleEntities(entities: schema.catalog.CatalogEntitySummary[]): Promise<schema.catalog.CatalogEntity[]> {
    return flatMap(entities, t => handleEntity(t))
  }

  async function handleEntity(entitySummary: schema.catalog.CatalogEntitySummary): Promise<schema.catalog.CatalogEntity[] | schema.catalog.CatalogEntity> {
    if (entitySummary.type === 'CONTAINER') {
      switch (entitySummary.containerType) {
        case 'SPACE':
        case 'FOLDER':
        case 'HOME': {
          const entity = await api.catalog.getEntity(entitySummary.id)
          switch (entity.entityType) {
            case 'home':
            case 'space':
            case 'folder': {
              return entity.children ? (await handleEntities(entity.children)).concat([await api.catalog.getEntity(entitySummary.id)]) : entity
            }
            default: {
              throw 'Unexpected'
            }
          }
        }
        case 'SOURCE': {
          return []
        }
        default: {
          throw 'Unexpected'
        }
      }
    } else {
      return await api.catalog.getEntity(entitySummary.id)
    }
  }

  return handleEntities(await api.catalog.getAllTopLevelContainers())
}

export type ClusterState = {
  users: ExistingUser[]
  catalog: schema.catalog.CatalogEntity[]
  sources: schema.source.Source[]
}

export async function getClusterState(api: API): Promise<ClusterState> {
  return {
    users: await api.user.getUsers(),
    catalog: await getEntities(api),
    sources: await api.source.getSources()
  }
}

function isClusterConfig(apiOrCluster: API | ClusterConfiguration): apiOrCluster is ClusterConfiguration {
  return (apiOrCluster as ClusterConfiguration).host !== undefined
}

function convertNonExpungedValue(v: any): any {
  if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null) {
    return v
  } else if (Array.isArray(v)) {
    return v.map(convertNonExpungedValue)
  } else {
    return expungeBadAttributes(v)
  }
}

function expungeBadAttributes(obj: any): any {
  const newObj: { [k: string]: any } = {}
  Object.keys(obj).forEach(k => {
    if (k !== 'uid' && k !== 'id' && k !== 'tag' && k !== 'version' && k !== 'modifiedAt' && k !== 'createdAt' && k !== 'state') {
      const v = obj[k]
      if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v === null) {
        newObj[k] = v
      } else if (Array.isArray(v)) {
        newObj[k] = v.map(convertNonExpungedValue)
      } else {
        newObj[k] = expungeBadAttributes(v)
      }
    }
  })

  return newObj
}

export function setClusterState(clusterState: ClusterState, sourceCredentials: { [sourceId: string]: object }, failOnError: boolean, stdout: Writable, stderr: Writable | null, apiOrUsers: API): Promise<void>
export function setClusterState(clusterState: ClusterState, sourceCredentials: { [sourceId: string]: object }, failOnError: boolean, stdout: Writable, stderr: Writable | null, cluster: ClusterConfiguration, users: schema.LoginBodyArgs[], adminUser: string, createFirstUser?: boolean): Promise<void>
export async function setClusterState(clusterState: ClusterState, sourceCredentials: { [sourceId: string]: object }, failOnError: boolean, stdout: Writable, stderr: Writable | null, apiOrCluster: API | ClusterConfiguration, users?: schema.LoginBodyArgs[], adminUserName?: string, _createFirstUser?: boolean) {
  clusterState = expungeBadAttributes(clusterState)

  let api: API
  if (isClusterConfig(apiOrCluster)) {
    const adminUserLogin = (users as schema.LoginBodyArgs[]).filter(u => u.userName === adminUserName)[0]
    if (adminUserLogin === undefined) {
      throw `Admin user ${adminUserName} is not in the list of user credentials`
    }

    if (_createFirstUser) {
      let adminUser = clusterState.users.filter(u => u.userName === adminUserName)[0]
      if (adminUser === undefined) {
        throw `Admin user ${adminUserName} is not in the list of cluster users`
      }

      try {
        await createFirstUser(apiOrCluster, adminUser, adminUserLogin.password)
        stdout.write('Created first user: ' + adminUser.userName + EOL)
      } catch (e) {
        if (failOnError) {
          throw e
        } else if (stderr) {
          stderr.write('Error whilst creating first user: ' + adminUser.userName + EOL)
          stderr.write(e.message + EOL)
        }
      }

      api = await createAPI(apiOrCluster, adminUserLogin)

      await Promise.all(clusterState.users
        .filter(u => u.userName !== adminUserName && (users as schema.LoginBodyArgs[]).filter(_u => u.userName === _u.userName).length > 0)
        .map(u => [(users as schema.LoginBodyArgs[]).filter(_u => u.userName === _u.userName)[0].password, u] as [string, ExistingUser])
        .map(async ([password, user]) => {
          try {
            await api.user.createUser(user, password)
            stdout.write('Created user: ' + user.userName + EOL)
          } catch (e) {
            if (failOnError) {
              throw e
            } else if (stderr) {
              stderr.write('Error whilst creating user: ' + user.userName + EOL)
              stderr.write(e.message + EOL)
            }
          }
        }))
    } else {
      api = await createAPI(apiOrCluster, adminUserLogin)

      await Promise.all(clusterState.users
        .filter(u => (users as schema.LoginBodyArgs[]).filter(_u => u.userName === _u.userName).length > 0)
        .map(u => [(users as schema.LoginBodyArgs[]).filter(_u => u.userName === _u.userName)[0].password, u] as [string, ExistingUser])
        .map(async ([password, user]) => {
          try {
            await api.user.createUser(user, password)
            stdout.write('Created user: ' + user.userName + EOL)
          } catch (e) {
            if (failOnError) {
              throw e
            } else if (stderr) {
              stderr.write('Error whilst creating user: ' + user.userName + EOL)
              stderr.write(e.message + EOL)
            }
          }
        }))
    }
  } else {
    api = apiOrCluster
  }

  await Promise.all(clusterState.sources
    .map(async source => {
      source.config = Object.assign({}, source.config, sourceCredentials[source.name] || {})
      try {
        await api.source.createSource(source)
        stdout.write('Created source: ' + source.name + EOL)
      } catch (e) {
        if (failOnError) {
          throw e
        } else if (stderr) {
          stderr.write('Error whilst creating source: ' + source.name + EOL)
          stderr.write(e.message + EOL)
        }
      }
    }))

  type NestedEntities = { [name: string]: [schema.catalog.CatalogEntity, NestedEntities] }
  const nestedEntities: NestedEntities = {}

  const datasetsWithSQL: { [path: string]: [string[][], schema.catalog.Dataset, number] | false } = {}

  function pathToString(path: string[]): string {
    let s = ''

    path.forEach(p => {
      if (p.indexOf('.') !== -1) {
        p = '"' + p + '"'
      }
      if (s.length === 0) {
        s = p
      } else {
        s += '.' + p
      }
    })

    return s
  }

  const topLevelEntities = clusterState.catalog.filter(e => {
    switch (e.entityType) {
      case 'space': {
        return true
      }
      case 'home': {
        return false
      }
      case 'dataset': {
        if (e.path[0].charAt(0) === '@') {
          datasetsWithSQL[pathToString(e.path)] = false
          // TODO: For now we are not able to recreate home space datasets
          return false
        } else if (e.sql) {
          const dependencies = getDatasetsFromSQL(e.sql)
          datasetsWithSQL[pathToString(e.path)] = [dependencies, e, -1]
          return false
        }
      }
      default: {
        let ne: [schema.catalog.CatalogEntity, NestedEntities] = [undefined as any, nestedEntities]
        for (let i = 0; i < e.path.length; i++) {
          let _next = ne[1][e.path[i]]
          if (!_next) {
            _next = [undefined as any, {}]
            ne[1][e.path[i]] = _next
          }
          ne = _next
        }
        ne[0] = e

        return false
      }
    }
  })

  const datasetsByDepNumber: schema.catalog.Dataset[][] = [[]]
  function fillInDepNumber(paths: string[]) {
    for (let i = 0; i < paths.length; i++) {
      const path = paths[i]
      const ds = datasetsWithSQL[path]
      if (ds && ds[2] === -1) {
        if (ds[0].length === 0) {
          ds[2] = 0
          datasetsByDepNumber[0].push(ds[1])
        } else {
          const deps = ds[0].map(pathToString)
          fillInDepNumber(deps)
          let depNum = 0
          for (let j = 0; j < deps.length; j++) {
            const dep = datasetsWithSQL[deps[j]]
            if (dep === false) {
              datasetsWithSQL[path] = false
              break
            } else if (dep) {
              depNum += dep[2] + 1
            }
          }
          if (datasetsWithSQL[path] !== false) {
            ds[2] = depNum
            let datasetsForDepth = datasetsByDepNumber[depNum]
            if (!datasetsForDepth) {
              datasetsForDepth = []
              datasetsByDepNumber[depNum] = datasetsForDepth
            }
            datasetsForDepth.push(ds[1])
          }
        }
      }
    }
  }
  fillInDepNumber(Object.keys(datasetsWithSQL))

  async function addEntities(entities: schema.catalog.CatalogEntity[]) {
    await Promise.all(entities.map(async e => {
      let eName: string
      switch (e.entityType) {
        case 'space':
        case 'home': {
          eName = e.name
          break
        }
        default: {
          eName = pathToString(e.path)
        }
      }
      try {
        await api.catalog.createEntity(e)
        stdout.write('Created ' + e.entityType + ': ' + eName + EOL)
      } catch (err) {
        if (failOnError) {
          throw err
        } else if (stderr) {
          stderr.write('Error whilst creating ' + e.entityType + ': ' + eName + EOL)
          stderr.write(err.message + EOL)
        }
      }
    }))

    await Promise.all(entities.map(e => {
      let ne: [schema.catalog.CatalogEntity, NestedEntities]
      switch (e.entityType) {
        case 'space':
        case 'home': {
          ne = nestedEntities[e.name]
          break
        }
        default: {
          ne = [undefined as any, nestedEntities]
          for (let i = 0; i < e.path.length; i++) {
            ne = ne[1][e.path[i]]
          }
        }
      }
      if (ne) {
        return addEntities(Object.keys(ne[1]).map(k => ne[1][k][0]))
      } else {
        // This happens when a space is empty
        return Promise.resolve()
      }
    }))
  }

  await addEntities(topLevelEntities)

  for (let i = 0; i < datasetsByDepNumber.length; i++) {
    const datasetForDepth = datasetsByDepNumber[i]
    await Promise.all(datasetForDepth.map(async e => {
      try {
        await api.catalog.createEntity(e)
        stdout.write('Created dataset: ' + pathToString(e.path) + EOL)
      } catch (err) {
        if (failOnError) {
          throw err
        } else if (stderr) {
          stderr.write('Error whilst creating dataset: ' + pathToString(e.path) + EOL)
          stderr.write(err.message + EOL)
        }
      }
    }))
  }
}