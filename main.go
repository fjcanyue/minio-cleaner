package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Minio struct {
		Endpoint        string `yaml:"endpoint"`
		AccessKeyID     string `yaml:"accessKeyId"`
		SecretAccessKey string `yaml:"secretAccessKey"`
		UseSSL          bool   `yaml:"useSSL"`
		Bucket          string `yaml:"bucket"`
	}
	Cleanup struct {
		MaxAge  int64  `yaml:"maxAge"`  // 文件最大保留天数
		MinSize int64  `yaml:"minSize"` // 文件最小大小（字节）
		DryRun  bool   `yaml:"dryRun"`  // 是否仅预览不实际删除
		Workers int    `yaml:"workers"` // 并发工作协程数
		LogFile string `yaml:"logFile"` // 日志文件路径
	}
}

func loadConfig(configPath string) (*Config, error) {
	cfg := &Config{}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return cfg, nil
}

func setupLogging(logFile string) (*os.File, error) {
	if logFile == "" {
		return nil, nil
	}

	// 确保日志目录存在
	logDir := filepath.Dir(logFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("创建日志目录失败: %v", err)
	}

	// 打开日志文件
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开日志文件失败: %v", err)
	}

	// 设置日志输出到文件和控制台
	mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(mw)
	return f, nil
}

func main() {
	// 解析命令行参数
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	// 加载配置文件
	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 设置日志
	logFile, err := setupLogging(cfg.Cleanup.LogFile)
	if err != nil {
		log.Fatalf("设置日志失败: %v", err)
	}
	if logFile != nil {
		defer logFile.Close()
	}

	// 创建Minio客户端
	minioClient, err := minio.New(cfg.Minio.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.Minio.AccessKeyID, cfg.Minio.SecretAccessKey, ""),
		Secure: cfg.Minio.UseSSL,
	})
	if err != nil {
		log.Fatalf("创建Minio客户端失败: %v", err)
	}

	// 设置清理时间阈值
	thresholdTime := time.Now().AddDate(0, 0, -int(cfg.Cleanup.MaxAge))

	// 开始清理过程
	log.Printf("开始清理过程，阈值时间: %v, 最小文件大小: %.2f MB", thresholdTime, float64(cfg.Cleanup.MinSize)/1024/1024)
	if cfg.Cleanup.DryRun {
		log.Println("运行模式: 预览（不会实际删除文件）")
	}

	// 添加计数器
	var totalFiles int64
	var processedFiles int64
	var deletedFiles int64
	var deletedSize int64

	// 创建工作通道
	fileChan := make(chan minio.ObjectInfo, cfg.Cleanup.Workers*2)
	doneChan := make(chan struct{})

	// 启动进度报告协程
	go func() {
		for {
			time.Sleep(10 * time.Second)
			processed := atomic.LoadInt64(&processedFiles)
			total := atomic.LoadInt64(&totalFiles)
			deleted := atomic.LoadInt64(&deletedFiles)
			size := atomic.LoadInt64(&deletedSize)

			if total > 0 {
				progress := float64(processed) / float64(total) * 100
				log.Printf("进度: %.2f%% (已处理: %d, 总数: %d, 已删除: %d, 已删除大小: %.2f MB)", 
					progress, processed, total, deleted, float64(size)/1024/1024)
			}

			// 如果所有文件都处理完成，退出进度报告
			if processed >= total && total > 0 {
				break
			}
		}
	}()

	// 启动工作协程
	for i := 0; i < cfg.Cleanup.Workers; i++ {
		go func() {
			for obj := range fileChan {
				// 检查文件大小
				if obj.Size < cfg.Cleanup.MinSize {
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				// 检查文件时间
				if obj.LastModified.After(thresholdTime) {
					atomic.AddInt64(&processedFiles, 1)
					continue
				}

				// 记录要删除的文件
				log.Printf("发现需要清理的文件: %s (大小: %.2f MB, 修改时间: %v)", 
					obj.Key, float64(obj.Size)/1024/1024, obj.LastModified)

				// 如果不是预览模式，执行删除
				if !cfg.Cleanup.DryRun {
					err := minioClient.RemoveObject(context.Background(), cfg.Minio.Bucket, obj.Key, minio.RemoveObjectOptions{})
					if err != nil {
						log.Printf("删除文件失败 %s: %v", obj.Key, err)
					} else {
						log.Printf("成功删除文件: %s", obj.Key)
						atomic.AddInt64(&deletedFiles, 1)
						atomic.AddInt64(&deletedSize, obj.Size)
					}
				}
				atomic.AddInt64(&processedFiles, 1)
			}
		}()
	}

	// 遍历存储桶中的所有对象
	objectCh := minioClient.ListObjects(context.Background(), cfg.Minio.Bucket, minio.ListObjectsOptions{
		Recursive: true,
	})

	// 发送对象到工作通道
	go func() {
		// 先统计总文件数
		var count int64
		for obj := range objectCh {
			if obj.Err != nil {
				log.Printf("列举对象时发生错误: %v", obj.Err)
				continue
			}
			count++
		}
		atomic.StoreInt64(&totalFiles, count)
		log.Printf("总文件数: %d", count)

		// 重新列举对象用于处理
		objectCh = minioClient.ListObjects(context.Background(), cfg.Minio.Bucket, minio.ListObjectsOptions{
			Recursive: true,
		})
		for obj := range objectCh {
			if obj.Err != nil {
				log.Printf("列举对象时发生错误: %v", obj.Err)
				continue
			}
			fileChan <- obj
		}
		close(fileChan)
		doneChan <- struct{}{}
	}()

	// 等待所有工作完成
	<-doneChan
	log.Printf("清理过程完成。总文件数: %d, 已处理: %d, 已删除: %d, 已删除大小: %.2f MB", 
		atomic.LoadInt64(&totalFiles), 
		atomic.LoadInt64(&processedFiles), 
		atomic.LoadInt64(&deletedFiles),
		float64(atomic.LoadInt64(&deletedSize))/1024/1024)
}
