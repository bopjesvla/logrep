// adapted from kibae's pg-logical-replication

start = table / trx

table = "table" space s:schema "." t:tablename ":" space a:action ":" d:data {return {schema:s, table:t, action:a, data:d};}
trx = trx_begin / trx_commit

trx_begin = "BEGIN" id:trx_id? {return {trx:"BEGIN", xid:id};}
trx_commit = "COMMIT" id:trx_id? tm:trx_tm? {return {trx:"COMMIT", xid:id, time:tm};}

trx_id = space t:[0-9]+ {return t.join('');}
trx_tm = space "(at " t:[^\)]+ ")" {return t.join('');}

space = " "+
empty = ""

schema = double_quote_escaped_string / dot_terminated_string
tablename = double_quote_escaped_string / colon_terminated_string
fieldname = double_quote_escaped_string / braket_start_terminated_string
datatype = "[" t:braket_end_terminated_string "]" {return t;}
value = "null" {return null;} / single_quote_escaped_string / space_terminated_string

double_quote_escaped_string = "\"" t:([^"]+ / [\r\n] / "\"\"" {return "\""})* "\"" {return t[0] ? t[0].join('') : '';}
single_quote_escaped_string = "'" t:([^']+ / [\r\n] / "''" {return "'";} )* "'" {return t[0] ? t[0].join('') : '';}
dot_terminated_string = t:[^.]+ {return t.join('');}
space_terminated_string = t:[^ \t\r\n]+ {return +t.join('');}
colon_terminated_string = t:[^:]+ {return t.join('');}
braket_start_terminated_string = t:[^\[]+ {return t.join('');}
braket_end_terminated_string = t:[^\]]+ {return t.join('');}

action = t:("INSERT" / "UPDATE" / "DELETE") {return t;}
datum = space f:fieldname t:datatype ":" v:value {return {name:f, type:t, value:v};}
data = space "(" t:[^))]+ ")" {return {type:"null", name:"message", value:t.join('')};} / datum+
