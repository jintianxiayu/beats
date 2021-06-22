// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package fileout2

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/common/file"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/publisher"
)

func init() {
	outputs.RegisterType("file2", makeFileout)
}

type fileOutput struct {
	log               *logp.Logger
	beat              beat.Info
	observer          outputs.Observer
	codec             codec.Codec
	batchSize         int
	containerRotators map[string]*file.Rotator
}

// makeFileout instantiates a new file output instance.
func makeFileout(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *common.Config,
) (outputs.Group, error) {
	config := defaultConfig()
	if err := cfg.Unpack(&config); err != nil {
		return outputs.Fail(err)
	}

	// disable bulk support in publisher pipeline
	cfg.SetInt("bulk_max_size", -1, -1)
	fo := &fileOutput{
		log:               logp.NewLogger("file2"),
		beat:              beat,
		observer:          observer,
		batchSize:         config.BatchSize,
		containerRotators: make(map[string]*file.Rotator),
	}
	if err := fo.init(beat, config); err != nil {
		return outputs.Fail(err)
	}

	// fo 是 Client接口的实现
	return outputs.Success(config.BatchSize, 0, fo)
}

func (out *fileOutput) init(beat beat.Info, c config) error {
	var err error
	rg := RotateGrade[c.RotateGrade]

	// 一个container对应一个rotator
	// publish的时候根据container查找rotator，然后write
	for i := range c.Containers {
		cName := c.Containers[i]
		filename := strings.Replace(c.Filename, "$CONTAINER$", cName, 1)
		path := filepath.Join(c.Path, filename)
		out.containerRotators[cName], err = file.NewFileRotator(
			path,
			file.Suffix(c.Suffix),
			file.MaxSizeBytes(c.RotateEveryKb*1024),
			file.MaxBackups(c.NumberOfFiles),
			file.Permissions(os.FileMode(c.Permissions)),
			file.WithLogger(logp.NewLogger("rotator").With(logp.Namespace("rotator"))),
			file.RotateOnStartup(c.RotateOnStartup),
			file.Interval(rg),
		)

		if err != nil {
			return err
		}

		out.log.Infof("Initialized file2 output. "+
			"path=%v max_size_bytes=%v max_backups=%v permissions=%v rotate_on_startup=%v",
			path, c.RotateEveryKb*1024, c.NumberOfFiles, os.FileMode(c.Permissions), c.RotateOnStartup)
	}

	if err != nil {
		return err
	}

	out.codec, err = codec.CreateEncoder(beat, c.Codec)
	if err != nil {
		return err
	}

	return nil
}

// Implement Outputer
func (out *fileOutput) Close() error {
	var err error
	for _, v := range out.containerRotators {
		err = v.Close()
		if err != nil {
			break
		}
	}
	return err
}

func getContainerName(event *publisher.Event, out *fileOutput) string {
	var cName string
	cVal, _ := event.Content.Fields.GetValue("container")
	if cVal != nil {
		if cMap, ok := cVal.(common.MapStr); ok {
			labelVal, _ := cMap.GetValue("labels")
			if labelVal != nil {
				if labelMap, ok := labelVal.(common.MapStr); ok {
					cNameVal := labelMap["io_kubernetes_container_name"]
					if cNameVal != nil {
						cName, _ = cNameVal.(string)
					}
				}
			}
		}
	}
	return cName

}

func (out *fileOutput) Publish(_ context.Context, batch publisher.Batch) error {
	defer batch.ACK()

	st := out.observer
	events := batch.Events()
	st.NewBatch(len(events))

	containerBufferData := make(map[string][]byte) // 每个容器缓存的event byte
	containerBufferCount := make(map[string]int)   // 每个容器缓存的event数量
	for k, _ := range out.containerRotators {
		containerBufferData[k] = make([]byte, 0)
		containerBufferCount[k] = 0
	}

	dropped := 0
	for i := range events {
		event := &events[i]

		serializedEvent, err := out.codec.Encode(out.beat.Beat, &event.Content)
		if err != nil {
			if event.Guaranteed() {
				out.log.Errorf("Failed to serialize the event: %+v", err)
			} else {
				out.log.Warnf("Failed to serialize the event: %+v", err)
			}
			out.log.Debugf("Failed event: %v", event)

			dropped++
			continue
		}

		cName := getContainerName(event, out)
		rotator := out.containerRotators[cName]
		if rotator == nil {
			dropped++
			continue
		}

		containerBufferCount[cName]++
		containerBufferData[cName] = append(append(containerBufferData[cName], serializedEvent...), '\n')
		bufferCount := containerBufferCount[cName]
		if bufferCount >= out.batchSize {
			if _, err = rotator.Write(containerBufferData[cName]); err != nil {
				st.WriteError(err)

				if event.Guaranteed() {
					out.log.Errorf("Writing event to file failed with: %+v", err)
				} else {
					out.log.Warnf("Writing event to file failed with: %+v", err)
				}

				dropped += bufferCount
				containerBufferData[cName] = make([]byte, 0)
				continue
			}
			st.WriteBytes(len(containerBufferData[cName]) + bufferCount)
			containerBufferData[cName] = make([]byte, 0)
			containerBufferCount[cName] = 0
		}
	}

	// 写出剩余的event
	for cName, remainCount := range containerBufferCount {
		remainEvents := containerBufferData[cName]
		rotator := out.containerRotators[cName]
		if remainCount > 0 {
			if _, err := rotator.Write(remainEvents); err != nil {
				st.WriteError(err)

				dropped += remainCount
				containerBufferData[cName] = make([]byte, 0)
				continue
			}
			st.WriteBytes(len(remainEvents) + remainCount)
		}
	}

	st.Dropped(dropped)
	st.Acked(len(events) - dropped)

	return nil
}

func (out *fileOutput) String() string {
	return "file2()"
}
