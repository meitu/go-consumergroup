# go-consumergroup [![Build Status](https://travis-ci.org/meitu/go-consumergroup.svg?branch=master)](https://travis-ci.org/meitu/go-consumergroup) [![Go Report Card](https://goreportcard.com/badge/github.com/meitu/go-consumergroup)](https://goreportcard.com/report/github.com/meitu/go-consumergroup)

### 简介
go-consumergroup是一款提供集群功能的kafka客户端，程序的实现是对sarama进行了一层包装，比其多提供了rebalance功能和自动管理offset的功能。目前基于zookeeper的集群功能已经开发完毕（适用于0.8及以后所有版本），基于kafka的集群功能还在开发中（适用于0.9.0.0及以上版本）。

### 依赖
<https://github.com/samuel/go-zookeeper>

<https://github.com/Shopify/sarama>

## 快速上手 

* API 文档请参照 [godoc](https://godoc.org/github.com/meitu/go-consumergroup).
* 使用例子参照 example 目录的 example.go 实现 [example](example/example.go)

## 测试

```shell
$ make test
```

***NOTE: *** 跑测试用例需要预先安装 docker-compose
