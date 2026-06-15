#!/bin/bash

node producer_http.js &
node producer_errores.js &
node producer_recursos.js &
node producer_red.js &
node producer_seguridad.js &

wait