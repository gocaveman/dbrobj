package dbrobj

// make a time type that persists properly
// notes:
// - the problem is likely not so much the Go type but the underlying database type to map to
// - in MySQL, DATETIME *may* be fine, but we need to confirm that driver stores in UTC and not local time
// - in SQLite it's between TEXT and INTEGER - integer is more compact but only has second precision, need to confirm driver with TEXT will store as UTC not local time
// - in Postgres it's probably TIMESTAMP (millisecond precision, no time zone)
// - the result of figuring this out should be clearly documented, even in 2018, dates are still freak'n mess.
//   But there probably are some simple 1,2,3 rules that if you follow it will be correct.  Document them in the README and GoDoc.

// and a Timestamps thing you can embed that has the hooks for creation and update
