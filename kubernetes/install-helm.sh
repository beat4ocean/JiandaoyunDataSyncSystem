#!/bin/bash

helm create namespace jdy-data-sync-system

kubectl create secret generic jdy-env-secret --from-env-file=.env -n jdy-data-sync-system

helm upgrade --install jdy-data-sync-system -n jdy-data-sync-system helm --create-namespace