import { effectScope, nextTick, ref, type EffectScope } from 'vue'
import { flushPromises } from '@vue/test-utils'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { GroupRule } from '@bookorbit/types'

const apiMock = vi.hoisted(() => vi.fn<(url: string, init?: RequestInit) => Promise<Response>>())
vi.mock('@/lib/api', () => ({ api: apiMock }))
vi.mock('@/features/scanner/composables/useScanProgress', () => ({ useScanProgress: () => ({ isScanning: () => false }) }))

import { useLibraryFacets } from '../useLibraryFacets'
import { useLibraryHeaderFacts } from '../useLibraryHeaderFacts'

describe('library statistics wait for the book listing', () => {
  let scope: EffectScope

  beforeEach(() => {
    vi.useFakeTimers()
    apiMock.mockReset()
    apiMock.mockResolvedValue({ ok: true, json: async () => ({ total: 7, libraryTotal: 7, formats: [], totalSizeBytes: 1234 }) } as Response)
    scope = effectScope()
  })

  afterEach(() => {
    scope.stop()
    vi.useRealTimers()
  })

  it('defers facets until cards are ready and uses the latest search without refetching for every page', async () => {
    const ready = ref(false)
    const query = ref('old')
    const facets = scope.run(() => useLibraryFacets(ref(3), ref<GroupRule>(), query, ready))!
    query.value = 'latest'
    await nextTick()
    await vi.advanceTimersByTimeAsync(200)
    expect(apiMock).not.toHaveBeenCalled()
    ready.value = true
    await nextTick()
    await flushPromises()
    expect(apiMock).toHaveBeenCalledTimes(1)
    expect(JSON.parse(String(apiMock.mock.calls[0]![1]?.body))).toEqual({ q: 'latest' })
    expect(facets.facets.value?.total).toBe(7)
    ready.value = false
    await nextTick()
    ready.value = true
    await nextTick()
    await flushPromises()
    expect(apiMock).toHaveBeenCalledTimes(1)
  })

  it('queues changed filters while cards load, then replaces the counts once', async () => {
    const ready = ref(true)
    const filter = ref<GroupRule>()
    scope.run(() => useLibraryFacets(ref(3), filter, ref(''), ready))
    await flushPromises()
    ready.value = false
    filter.value = { type: 'group', join: 'AND', rules: [] }
    await nextTick()
    await vi.advanceTimersByTimeAsync(200)
    expect(apiMock).toHaveBeenCalledTimes(1)
    ready.value = true
    await nextTick()
    await flushPromises()
    expect(apiMock).toHaveBeenCalledTimes(2)
    expect(JSON.parse(String(apiMock.mock.calls[1]![1]?.body))).toEqual({ filter: { type: 'group', join: 'AND', rules: [] } })
  })

  it('defers capacity statistics and clears the old library size on navigation', async () => {
    const ready = ref(false)
    const libraryId = ref<number | null>(3)
    const facts = scope.run(() => useLibraryHeaderFacts(libraryId, ref(false), ready))!
    expect(apiMock).not.toHaveBeenCalled()
    ready.value = true
    await nextTick()
    await flushPromises()
    expect(apiMock).toHaveBeenCalledWith('/api/v1/libraries/3/stats')
    expect(facts.sizeBytes.value).toBe(1234)
    ready.value = false
    libraryId.value = 4
    await nextTick()
    expect(facts.sizeBytes.value).toBeNull()
    expect(apiMock).toHaveBeenCalledTimes(1)
    ready.value = true
    await nextTick()
    await flushPromises()
    expect(apiMock).toHaveBeenCalledWith('/api/v1/libraries/4/stats')
  })
})
