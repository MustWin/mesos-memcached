#!/bin/bash

go build -o pkg/scheduler ./scheduler && 
go build -o pkg/executor ./executor

