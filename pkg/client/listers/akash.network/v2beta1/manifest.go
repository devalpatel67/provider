/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by lister-gen. DO NOT EDIT.

package v2beta1

import (
	v2beta1 "github.com/akash-network/provider/pkg/apis/akash.network/v2beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"
)

// ManifestLister helps list Manifests.
// All objects returned here must be treated as read-only.
type ManifestLister interface {
	// List lists all Manifests in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v2beta1.Manifest, err error)
	// Manifests returns an object that can list and get Manifests.
	Manifests(namespace string) ManifestNamespaceLister
	ManifestListerExpansion
}

// manifestLister implements the ManifestLister interface.
type manifestLister struct {
	indexer cache.Indexer
}

// NewManifestLister returns a new ManifestLister.
func NewManifestLister(indexer cache.Indexer) ManifestLister {
	return &manifestLister{indexer: indexer}
}

// List lists all Manifests in the indexer.
func (s *manifestLister) List(selector labels.Selector) (ret []*v2beta1.Manifest, err error) {
	err = cache.ListAll(s.indexer, selector, func(m interface{}) {
		ret = append(ret, m.(*v2beta1.Manifest))
	})
	return ret, err
}

// Manifests returns an object that can list and get Manifests.
func (s *manifestLister) Manifests(namespace string) ManifestNamespaceLister {
	return manifestNamespaceLister{indexer: s.indexer, namespace: namespace}
}

// ManifestNamespaceLister helps list and get Manifests.
// All objects returned here must be treated as read-only.
type ManifestNamespaceLister interface {
	// List lists all Manifests in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v2beta1.Manifest, err error)
	// Get retrieves the Manifest from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v2beta1.Manifest, error)
	ManifestNamespaceListerExpansion
}

// manifestNamespaceLister implements the ManifestNamespaceLister
// interface.
type manifestNamespaceLister struct {
	indexer   cache.Indexer
	namespace string
}

// List lists all Manifests in the indexer for a given namespace.
func (s manifestNamespaceLister) List(selector labels.Selector) (ret []*v2beta1.Manifest, err error) {
	err = cache.ListAllByNamespace(s.indexer, s.namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v2beta1.Manifest))
	})
	return ret, err
}

// Get retrieves the Manifest from the indexer for a given namespace and name.
func (s manifestNamespaceLister) Get(name string) (*v2beta1.Manifest, error) {
	obj, exists, err := s.indexer.GetByKey(s.namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(v2beta1.Resource("manifest"), name)
	}
	return obj.(*v2beta1.Manifest), nil
}
