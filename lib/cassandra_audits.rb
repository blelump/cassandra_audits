module CassandraAudits

  class << self
    attr_accessor :ignored_attributes, :current_user_method, :audit_class, :partition_key_provider, :superior_user_method, :partition_key_name
    
    def store
      Thread.current[:audited_store] ||= {}
    end
  end
  
  @ignored_attributes = %w(lock_version created_at updated_at created_on updated_on)
  
  @current_user_method = :current_user
  @superior_user_method = :superior_user
  @partition_key_name = :partition_key
  @partition_key_provider = lambda { |controller| controller.send(:current_user).department_id }
end

require 'cassandra_audits/adapters/active_record'