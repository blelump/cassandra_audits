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
      class UserInfo
        include ::ActiveModel::Observing
        include ::ActiveSupport::Callbacks

        def self.before_create(arg)
        end

        attr_accessor :audit_data

        def initialize(audit_data = {}, decode = false)
          self.audit_data = OpenStruct.new(audit_data)

        end

        def take
          notify_observers(:before_create)
          audit_data.marshal_dump
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

      end
    end
  end
end
