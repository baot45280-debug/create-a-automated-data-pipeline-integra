require 'json'
require 'httparty'
require 'csv'

class Automator
  def initialize(config)
    @config = config
    @data_sources = config['data_sources']
    @pipeline = config['pipeline']
  end

  def run
    data = fetch_data
    transform_data(data)
    load_to_destination(data)
  end

  private

  def fetch_data
    data = []
    @data_sources.each do |source|
      case source['type']
      when 'api'
        response = HTTParty.get(source['url'])
        data.concat(parse_api_response(response, source['mapping']))
      when 'csv'
        csv_data = CSV.read(source['file_path'], headers: true)
        data.concat(parse_csv_data(csv_data, source['mapping']))
      end
    end
    data
  end

  def parse_api_response(response, mapping)
    response.parsed_response.map do |item|
      mapped_item = {}
      mapping.each do |key, value|
        mapped_item[key] = item[value]
      end
      mapped_item
    end
  end

  def parse_csv_data(csv_data, mapping)
    csv_data.map do |row|
      mapped_row = {}
      mapping.each do |key, value|
        mapped_row[key] = row[value]
      end
      mapped_row
    end
  end

  def transform_data(data)
    transformed_data = []
    data.each do |item|
      transformed_item = {}
      @pipeline['transforms'].each do |transform|
        transformed_item[transform['output']] = transform['function'].call(item)
      end
      transformed_data << transformed_item
    end
    transformed_data
  end

  def load_to_destination(data)
    case @pipeline['destination']['type']
    when 'csv'
      CSV.open(@pipeline['destination']['file_path'], 'w', headers: true) do |csv|
        csv << data.first.keys
        data.each do |item|
          csv << item.values
        end
      end
    when 'api'
      response = HTTParty.post(@pipeline['destination']['url'], body: data.to_json)
      raise 'Error loading data to API' unless response.success?
    end
  end
end

config = {
  'data_sources' => [
    { 'type' => 'api', 'url' => 'https://api.example.com/data', 'mapping' => { 'id' => 'id', 'name' => 'name' } },
    { 'type' => 'csv', 'file_path' => 'data.csv', 'mapping' => { 'id' => 'id', 'name' => 'name' } }
  ],
  'pipeline' => {
    'transforms' => [
      { 'output' => 'full_name', 'function' => proc { |item| item['first_name'] + ' ' + item['last_name'] } }
    ],
    'destination' => { 'type' => 'csv', 'file_path' => 'output.csv' }
  }
}

automator = Automator.new(config)
automator.run