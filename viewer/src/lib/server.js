const api_host = "http://localhost:8000/"

export const getApi = async (api_data_type, api_data_id=0) => {
    let api_url = api_host + 'data/' + api_data_type
    if (api_data_id === 0) {
        return
    } else if (api_data_id > 0) {
        api_url += "/" + api_data_id
    }

    const response = await fetch(api_url)
    const data = response.json()

    return data
}