<script>
  import { onMount } from 'svelte'
  import Chart from 'chart.js/auto/auto'
  import 'chartjs-adapter-moment'

  import { getApi } from './lib/server'
  import { metrics } from './lib/metric'


  const interval = 10000
  const initChartFrame = {
    type: 'line',
    data: {
      labels: [],
      datasets: [
        { label: '', data: [] }
      ]
    },
    options: {
      scales: {
        x: { 
          type: 'timeseries'
        }
      }
    }
  }

  let currentTime = ''
  let currentNode = ''
  let currentPod = ''
  let tableData = {
    'overall': [],
    'nodesysco': [],
    'pod': [],
    'container': [],
    'podnet': [],
    'podvol': [],
    'podts': []
  }

  let api_data_ids = {
    "nodeid": 0,
    "podid": 0
  }

  let charts = {
    podcpu: undefined,
    podmemory: undefined
  }

  let podcpuChart
  let podmemoryChart

  onMount(() => {
    updateDataTable()

    charts.podcpu = new Chart(podcpuChart.getContext('2d'), initChartFrame)
    charts.podmemory = new Chart(podmemoryChart.getContext('2d'), initChartFrame)
  })

  async function updateDataTable() {
    await setDataTable('overall', -1)
    await updateNodeTable()
    
    setTimeout(updateDataTable, interval)
  }

  async function setDataTable(dataType, dataId=0) {    
    const data = await getApi(dataType, dataId)
    tableData[dataType] = data
    
    if (dataType === 'overall') {
      const currentTs = data[0]._ontunetime
      const currentTsDate = new Date(currentTs * 1000)
      currentTime = currentTsDate.toISOString().replace('T',' ' ).substring(0, 19)
    }
  }

  async function updateNodeTable() {
    await setDataTable('nodesysco', api_data_ids.nodeid)
    await setDataTable('pod', api_data_ids.nodeid)
    await setDataTable('podts', api_data_ids.nodeid)

    updateNodeChart('podcpu','_cpuusage')
    updateNodeChart('podmemory','_memoryused')
  }

  async function updateNodeChart(chartId, keyColumn) {
    const data = tableData.podts
    let labels = []
    let values = {}

    if (!data) return

    data.forEach((row) => {
        const podname = row._podname
        const ontunetime = row._ontunetime

        if (!(podname in values)) values[podname] = []
        if (!(labels.includes(ontunetime))) labels.push(ontunetime)

        values[podname].push(row[keyColumn])
    })

    charts[chartId].data.labels.pop()
    charts[chartId].data.datasets.forEach((dataset) => {
      dataset.data.pop()
    })

    charts[chartId].data.labels = labels.map((el) => new Date(el * 1000).toISOString().replace('T', ' ').substring(0,19))
    charts[chartId].data.datasets = Object.keys(values).map((el) => {
        return {
            label: el,
            data: values[el]
        }
    })    
    charts[chartId].options = {
        responsive: true
    }

    charts[chartId].update()
  }

  async function updatePodTable() {
    await setDataTable('container', api_data_ids.podid)
    await setDataTable('podnet', api_data_ids.podid)
    await setDataTable('podvol', api_data_ids.podid)  
  }

  function selectNode(id) {
    api_data_ids.nodeid = id
    currentNode = tableData.overall.filter(el => el._nodeid === id)[0]._nodename
    updateNodeTable()
  }

  function selectPod(id) {
    api_data_ids.podid = id
    currentPod = tableData.pod.filter(el => el._podid === id)[0]._podname
    updatePodTable()
  }
</script>

<main>
  <h2>onTune Kube Manager Prototype Alpha ver0.2</h2>
  <h3>Node Performance Table</h3>
  <p>Time: {currentTime}</p>

  <table>
    <thead>
    <tr>
      {#each metrics.overall as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>
    {#if tableData.overall}
    {#each tableData.overall as row}
    <tr on:click={() => selectNode(row._nodeid)}>
      <td>{row._nodeid}</td>
      <td>{row._nodename}</td>
      <td>{row._cpuusage}</td>
      <td>{row._memoryused}</td>
      <td>{row._swapused}</td>
      <td>{row._memorysize}</td>
      <td>{row._swapsize}</td>
      <td>{row._netusage}</td>
      <td>{row._fsusage}</td>
      <td>{row._fssize}</td>
      <td>{row._fsiusage}</td>
      <td>{row._imgfsusage}</td>
      <td>{row._proccount}</td>
    </tr>
    {/each}
    {/if}
    </tbody>
  </table>

  <h3>Node System Container Table - {currentNode}</h3>
  <table>
    <thead>
    <tr>
      {#each metrics.nodesysco as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>      
      {#if tableData.nodesysco}
      {#each tableData.nodesysco as row}
      <tr>
        <td>{row._containername}</td>
        <td>{row._cpuusage}</td>
        <td>{row._memoryused}</td>
        <td>{row._swapused}</td>
        <td>{row._memorysize}</td>
        <td>{row._swapsize}</td>
      </tr>
      {/each}
      {/if}  
    </tbody>
  </table>

  <h3>Pod Performance Table - {currentNode}</h3>
  <table>
    <thead>
    <tr>
      {#each metrics.pod[0] as {spanType, span, name}}
      {#if spanType === 'column'}
      <th colspan={span}>{name}</th>
      {:else}
      <th rowspan={span}>{name}</th>
      {/if}
      {/each}
    </tr>
    <tr>
      {#each metrics.pod[1] as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>      
    {#if tableData.pod}
    {#each tableData.pod as row}
    <tr on:click={() => selectPod(row._podid)}>
      <td>{row._podid}</td>
      <td>{row._podname}</td>
      <td>{row._cpuusage}</td>
      <td>{row._memoryused}</td>
      <td>{row._swapused}</td>
      <td>{row._memorysize}</td>
      <td>{row._swapsize}</td>
      <td>{row._netusage}</td>
      <td>{row._netrxrate}</td>
      <td>{row._nettxrate}</td>
      <td>{row._netrxerrors}</td>
      <td>{row._nettxerrors}</td>
      <td>{row._volused}</td>
      <td>{row._voliused}</td>
      <td>{row._epstused}</td>
      <td>{row._epstiused}</td>
      <td>{row._proccount}</td>
    </tr>
    {/each}
    {/if}
    </tbody>
  </table>

  <h3>Pod Chart - CPU Usage(%) - {currentNode}</h3>
  <div>
  <canvas width={1000} height={400} bind:this={podcpuChart} />
  </div>
  <hr />
  <h3>Pod Chart - Memory Used(%) - {currentNode}</h3>
  <div>
  <canvas width={1000} height={400} bind:this={podmemoryChart} />
  </div>
  <hr />

  <h3>Pod Container Table - {currentPod}</h3>
  <table>
    <thead>
    <tr>
      {#each metrics.container[0] as {spanType, span, name}}
      {#if spanType === 'column'}
      <th colspan={span}>{name}</th>
      {:else}
      <th rowspan={span}>{name}</th>
      {/if}
      {/each}
    </tr>
    <tr>
      {#each metrics.container[1] as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>      
    {#if tableData.container}
    {#each tableData.container as row}
    <tr>
      <td>{row._containerid}</td>
      <td>{row._containername}</td>
      <td>{row._cpuusage}</td>
      <td>{row._memoryused}</td>
      <td>{row._swapused}</td>
      <td>{row._memorysize}</td>
      <td>{row._swapsize}</td>
      <td>{row._rootfsused}</td>
      <td>{row._rootfsiused}</td>
      <td>{row._logfsused}</td>
      <td>{row._logfsiused}</td>
    </tr>
    {/each}
    {/if}
    </tbody>
  </table>

  <h3>Pod Network Interface Performanc Table - {currentPod}</h3>
  <table>
    <thead>
    <tr>
      {#each metrics.podnet as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>      
    {#if tableData.podnet}
    {#each tableData.podnet as row}
    <tr>
      <td>{row._devicename}</td>
      <td>{row._netusage}</td>
      <td>{row._netrxrate}</td>
      <td>{row._nettxrate}</td>
      <td>{row._netrxerrors}</td>
      <td>{row._nettxerrors}</td>
    </tr>
    {/each}
    {/if}    
    </tbody>
  </table>  

  <h3>Pod Volume Interface Performanc Table - {currentPod}</h3>
  <table>
    <thead>
    <tr>
      {#each metrics.podvol as column}
      <th>{column}</th>
      {/each}
    </tr>
    </thead>
    <tbody>      
    {#if tableData.podvol}
    {#each tableData.podvol as row}
    <tr on:click={() => selectPod(row._podid)}>
      <td>{row._devicename}</td>
      <td>{row._volused}</td>
      <td>{row._voliused}</td>
    </tr>
    {/each}
    {/if}
    </tbody>
  </table>  

</main>

<style>
  table {
    width: 100%;
    margin: 10px 20px;
    border: 1px solid #777777;
    border-spacing: 0;
    border-collapse: collapse;
  }

  th, td {
    border: 1px solid #AAAAAA;
  }
</style>