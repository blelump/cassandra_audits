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
      class Audit
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
#          self.send :set_audit_user
        end
        
        def save
          notify_observers(:before_create)
          begin 
            [audit_data.send(CassandraAudits.partition_key_name)].flatten.compact.uniq.each do |partition_key|
              CassandraMigrations::Cassandra.write!(:audits, audit_data.marshal_dump.merge(CassandraAudits.partition_key_name => partition_key))
            end
          rescue Cql::QueryError => e
            ::ActiveRecord::Base.logger.info("Cql::QueryError")
            ::ActiveRecord::Base.logger.info(e.inspect)
            ::ActiveRecord::Base.logger.info( audit_data.marshal_dump)
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
        
        
        private 
        def set_audit_user
          self.user = Thread.current[:audited_user] if Thread.current[:audited_user]
          nil # prevent stopping callback chains
        end
        
        def decode_field(field)
          self.audit_data.send("#{field}=", self.audit_data.send(field).present? ? JSON.parse(self.audit_data.send(field)) : {}) 
        end
        
        
        #        include CassandraAudits::Audit
        #
        #
        #        serialize :audited_changes
        #
        #        default_scope         order(:version)
        #        scope :descending,    reorder("version DESC")
        #        scope :creates,       :conditions => {:action => 'create'}
        #        scope :updates,       :conditions => {:action => 'update'}
        #        scope :destroys,      :conditions => {:action => 'destroy'}
        #
        #        scope :up_until,      lambda {|date_or_time| where("created_at <= ?", date_or_time) }
        #        scope :from_version,  lambda {|version| where(['version >= ?', version]) }
        #        scope :to_version,    lambda {|version| where(['version <= ?', version]) }
        #
        #        # Return all audits older than the current one.
        #        def ancestors
        #          self.class.where(['auditable_id = ? and auditable_type = ? and version <= ?',
        #            auditable_id, auditable_type, version])
        #        end
        #
        #        # Allows user to be set to either a string or an ActiveRecord object
        #        # @private
        #        def user_as_string=(user)
        #          # reset both either way
        #          self.user_as_model = self.username = nil
        #          user.is_a?(::ActiveRecord::Base) ?
        #            self.user_as_model = user :
        #            self.username = user
        #        end
        #        alias_method :user_as_model=, :user=
        #        alias_method :user=, :user_as_string=
        #
        #        # @private
        #        def user_as_string
        #          self.user_as_model || self.username
        #        end
        #        alias_method :user_as_model, :user
        #        alias_method :user, :user_as_string
        #
        #      private
        #        def set_version_number
        #          max = self.class.maximum(:version,
        #            :conditions => {
        #              :auditable_id => auditable_id,
        #              :auditable_type => auditable_type
        #            }) || 0
        #          self.version = max + 1
        #        end
      
      end
    end
  end
end
