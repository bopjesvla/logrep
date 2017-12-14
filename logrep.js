process.on("unhandledRejection", e => {
    console.trace(e)
    throw e
})

let pg = require('pg')
let fs = require('fs')
let QueryStream = require('pg-query-stream')
let LogicalReplication = require('pg-logical-replication')
let PluginTestDecoding = LogicalReplication.LoadPlugin('output/test_decoding')
let decoder = require('./decode.out.js')
let start_rethinkdb = require('./start_rdb.js')
let r = require('rethinkdb')
let pkeyQuery = fs.readFileSync(__dirname + '/pkey.sql', 'utf8')
console.log(pkeyQuery)

let rep = async function (config) {
    let server = await start_rethinkdb(config.rdb)
    let conn = await server.connect()
    let pgConfig = config.pg || {}
    let pgClient = new pg.Client(pgConfig)
    await pgClient.connect()
    let db = pgConfig.database || pgConfig.user
    let dbList = await r.dbList().run(conn)

    if (config.reset && dbList.includes(db)) {
        await r.dbDrop(db).run(conn)
    }
    if (config.reset || !dbList.includes(db)) {
        await r.dbCreate(db).run(conn)
    }
    await conn.use(db)
    await r.db(db).wait().run(conn)

    let knownTables = await r.tableList().run(conn)
    let tableCreators = {}

    console.log(knownTables)

    let i = 0
    let lastLsn = null
    let consumedI = null

    if (!config.reset)
        try {
            lastLsn = fs.readFileSync('.lsn')
        } catch (e) {}

    let copyTable = async (pgSchema, pgTable, rTable) => {
        console.log(rTable)
        let res = await pgClient.query(pkeyQuery, [pgSchema, pgTable])
        if (!res.rows || !res.rows.length)
            throw `${pgSchema}.${pgTable} has no primary key`
        await r.tableCreate(rTable, {primaryKey: res.rows[0].column_name}).run(conn)
        knownTables.push(rTable)
    }

    let stream = (new LogicalReplication(pgConfig))
        .on('data', async function(msg) {
            lastLsn = msg.lsn
            fs.writeFile('.lsn', lastLsn, e => {if (e) throw e})
            let log = (msg.log || '').toString('utf8')
            try {
                // console.log(log)
                let d = decoder.parse(log)
                console.log(d)
                if (d.schema && d.table) {
                    let table = d.schema == 'public' ? d.table : `${d.schema}_${d.table}`
                    if (!knownTables.includes(table)) {
                        let first = !tableCreators[table]

                        if (first) {
                            tableCreators[table] = copyTable(d.schema, d.table, table)
                        }
                        await tableCreators[table]
                    }
                    if (d.action == 'INSERT') {
                        let obj = {}
                        for (let {name, value} of d.data) {
                            obj[name] = value
                        }
                        await r.table(table).insert(obj).run(conn)
                    }
                }
            } catch (e) {
                throw e
                console.trace(log, e)
            }
        }).on('error', function(err) {
            console.trace('Error #2', err)
            setTimeout(proc, 1000)
        })

    ;(function proc() {
        stream.getChanges('test_slot', lastLsn, {
            includeXids: false, //default: false
            includeTimestamp: false, //default: false
        }, function(err) {
            if (err) {
                console.trace('Logical replication initialize error', err)
    			      setTimeout(proc, 1000)
    		    }
    	  })
    })()

}

let config = {reset: true, pg: {user: 'postgres', database: 'postgres'}}
rep(config)
