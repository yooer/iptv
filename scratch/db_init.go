package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func passwd(raw string) string {
	m1 := md5.Sum([]byte(raw))
	m2 := md5.Sum([]byte(raw + hex.EncodeToString(m1[:])))
	return hex.EncodeToString(m2[:])
}

func main() {
	uri := "mongodb://cloud:3Tm5tZ2KRSM87CpY@10.239.113.34:26110/cloud"
	dbName := "cloud"

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database(dbName)
	fmt.Printf("--- 数据库状态检查: %s ---\n", dbName)

	// 1. 获取所有集合
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		log.Fatalf("无法获取集合列表: %v", err)
	}

	for _, collName := range collections {
		fmt.Printf("\n[集合: %s]\n", collName)
		
		// 打印索引信息
		coll := db.Collection(collName)
		indexCursor, err := coll.Indexes().List(ctx)
		if err != nil {
			fmt.Printf("  无法获取索引信息: %v\n", err)
			continue
		}
		
		var indexes []bson.M
		if err := indexCursor.All(ctx, &indexes); err == nil {
			for _, idx := range indexes {
				fmt.Printf("  - 索引名: %v, 键: %v\n", idx["name"], idx["key"])
			}
		}

		// 如果是用户表，检查内容
		if collName == "Members" {
			count, _ := coll.CountDocuments(ctx, bson.M{})
			fmt.Printf("  - 当前记录数: %d\n", count)
			if count == 0 {
				fmt.Println("  - 检测到用户表为空，准备创建默认管理员...")
				defaultPass := passwd("e10adc3949ba59abbe56e057f20f883e") // 123456
				_, err := coll.InsertOne(ctx, bson.M{
					"mail":     "admin@iptv.com",
					"password": defaultPass,
					"level":    100,
					"name":     "管理员",
				})
				if err != nil {
					fmt.Printf("  - 创建管理员失败: %v\n", err)
				} else {
					fmt.Println("  - 默认管理员 (admin@iptv.com / 123456) 创建成功！")
				}
			}
		} else {
			count, _ := coll.CountDocuments(ctx, bson.M{})
			fmt.Printf("  - 当前记录数: %d\n", count)
		}
	}

	fmt.Println("\n--- 检查完成 ---")
}
