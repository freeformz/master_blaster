# Intro

Proof of concept to use postgres' listen/notify as a transport mechanism for feeding workers data.

It's probably horrible in different ways. ;-)

# Usage

`bundle install` first

in terminal #1 run: `DATABASE_URL=postgres://user:pwd@localhost/dbname bundle exec bin/mb`

in other terminals run: `DATABASE_URL=postgres://user:pwd@localhost/dbname bundle exec bin/worker`

to get a simple admin repl: `DATABASE_URL=postgres://user:pwd@localhost/dbname bundle exec bin/mb_repl.rb`

# Asumptions

it's meant to feed data to workers. It's up to the workers to verify that it can / should do the work and ensure locking.

it's not durable. It's assumed that the class that appends jobs to the queue either handles that or it's not necessary.

Some timeouts should be adjusted based on job length and/or worker count.
