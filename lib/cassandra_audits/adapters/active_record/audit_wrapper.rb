require 'set'
require 'benchmark'
require 'ostruct'
#require 'audit_data'

module CassandraAudits
  module Adapters
    module ActiveRecord
      # Audit saves the changes to ActiveRecord models.  It has the following attributes:
      #
      # * <tt>auditable</tt>: the ActiveRecord model that was changed
      # * <tt>user</tt>: the user that performed the change; a string or an ActiveRecord model
      # * <tt>action</tt>: one of create, update, or delete
      # * <tt>audited_changes</tt>: a serialized hash of all the changes
      # * <tt>comment</tt>: a comment set with the audit
      # * <tt>created_at</tt>: Time that the change was performed
      #
      class AuditWrapper
        include ::ActiveModel::Observing
        include ::ActiveSupport::Callbacks

        def self.before_create(arg)
        end

        attr_accessor :audit_data

        def initialize(audit_data = {}, decode = false)
          self.audit_data = OpenStruct.new(audit_data)
          if decode
            ["audit_destination_data", "audit_source_data", "audited_changes", "superior_data"].each do |field|
              send(:decode_field, field)
            end
          end
        end

        def save
          notify_observers(:before_create)
          begin
            CassandraMigrations::Cassandra.write!(table_name,
                                                  audit_data.marshal_dump.merge(partition_key))
          rescue Cql::QueryError => e
            ::ActiveRecord::Base.logger.debug("Cql::QueryError")
            ::ActiveRecord::Base.logger.debug(e.inspect)
            ::ActiveRecord::Base.logger.debug( audit_data.marshal_dump)
          end
        end

        def method_missing(method_name, *arguments, &block)
          audit_data.send(method_name)
        end

        def respond_to_missing?(method_name, include_private = false)
          audit_data.send(method_name).present? || super
        end



        # All audits made during the block called will be recorded as made
        # by +user+. This method is hopefully threadsafe, making it ideal
        # for background operations that require audit information.
        def as_user(user, &block)
          Thread.current[:audited_user] = user
          yield
        ensure
          Thread.current[:audited_user] = nil
        end

        protected

        private

        def set_audit_user
          self.user = Thread.current[:audited_user] if Thread.current[:audited_user]
          nil # prevent stopping callback chains
        end

        def decode_field(field)
          self.audit_data
          .send("#{field}=", self.audit_data.send(field).present? ? JSON.parse(self.audit_data.send(field)) : {})
        end


        def table_name
          raise "not implemented"
        end
      end
    end
  end
end
