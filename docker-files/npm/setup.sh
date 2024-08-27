#!/bin/sh

npm cache clean --force
rm -rf node_modules package-lock.json
npm install

exec "$@"
